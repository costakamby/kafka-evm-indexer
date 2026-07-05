package indexer.streamstopology.decode

import indexer.decoder.DecodeResult
import indexer.decoder.EventDecoder
import indexer.schema.DecodeFailureRecord
import indexer.schema.DecodedEventEnvelope
import indexer.schema.EventKey
import indexer.schema.RawLogRecord
import indexer.schema.SubscriptionRecord
import indexer.schema.SubscriptionStatus
import indexer.streamstopology.serde.jsonSerdeOf
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp

/**
 * Wires the decode step: raw-logs-topic + a lookup against the subscriptions
 * GlobalKTable's store -> decoded-logs-topic / decode-dead-letter-topic
 * (acceptance criterion 4.3). Pure topology-building code: no Kafka Streams
 * instance is created here, only Topology shape.
 *
 * Looks up the ACTIVE subscription for a raw log's (network, contractAddress)
 * by SCANNING the already-registered subscriptions GlobalKTable store (a
 * `KStream.process(...)` referencing that store by name), rather than via a
 * DSL stream-table join. A Kafka topic can only be registered as ONE kind of
 * source per topology (plain stream vs. GlobalKTable) - since subscription-api
 * needs subscriptions-topic as a GlobalKTable (so every instance holds the full
 * table for Interactive Queries, acceptance criterion 4.1), this step cannot
 * ALSO re-source it as an independent KStream for a join. A linear scan per
 * raw log is an acceptable cost at this project's scale (subscriptions are
 * control-plane data, not high-throughput).
 *
 * A raw log is dead-lettered (never silently dropped, never crashes the app)
 * when: no subscription exists for its (network, contractAddress); the
 * matching subscription is REMOVED, not ACTIVE; or the decoder itself can't
 * decode it (malformed topics, unknown event, etc - see decoder's EventDecoder).
 */
object DecodeTopology {

    fun addTo(
        builder: StreamsBuilder,
        subscriptionsStoreName: String,
        rawLogsTopic: String,
        decodedLogsTopic: String,
        deadLetterTopic: String,
        decoder: EventDecoder,
        clock: () -> Long = System::currentTimeMillis,
    ) {
        val stringSerde = Serdes.String()
        val decodedSerde = jsonSerdeOf(DecodedEventEnvelope.serializer())
        val failureSerde = jsonSerdeOf(DecodeFailureRecord.serializer())

        // NOTE: the subscriptions GlobalKTable store is deliberately NOT listed
        // here - Kafka Streams requires global stores to be left off a
        // Processor's explicit store list (they're auto-connected to every
        // processor in the topology); listing it explicitly is a topology
        // validation error ("can be used by a Processor without being
        // specified; it should not be explicitly passed").
        val decodeResults = builder
            .stream(rawLogsTopic, Consumed.with(stringSerde, jsonSerdeOf(RawLogRecord.serializer())))
            .process(
                ProcessorSupplier<String, RawLogRecord, String, DecodeResult> {
                    DecodeStepProcessor(subscriptionsStoreName, decoder, clock)
                },
                Named.`as`("decode-step"),
            )

        decodeResults
            .split(Named.`as`("decode-outcome-"))
            .branch(
                { _, result -> result is DecodeResult.Success },
                Branched.withConsumer { successes ->
                    successes.mapValues { (it as DecodeResult.Success).envelope }
                        .to(decodedLogsTopic, Produced.with(stringSerde, decodedSerde))
                },
            )
            .defaultBranch(
                Branched.withConsumer { failures ->
                    failures.mapValues { (it as DecodeResult.Failure).record }
                        .to(deadLetterTopic, Produced.with(stringSerde, failureSerde))
                },
            )
    }
}

/**
 * Resolves the ACTIVE subscription for a raw log's (network, contractAddress)
 * by scanning the subscriptions GlobalKTable store, then delegates to
 * [EventDecoder]. Rekeys every output to [EventKey] regardless of outcome.
 */
private class DecodeStepProcessor(
    private val subscriptionsStoreName: String,
    private val decoder: EventDecoder,
    private val clock: () -> Long,
) : Processor<String, RawLogRecord, String, DecodeResult> {

    private lateinit var context: ProcessorContext<String, DecodeResult>

    // Every KTable/GlobalKTable materialization in the Kafka Streams DSL uses a
    // TIMESTAMPED store internally (KIP-258), regardless of what StoreSupplier
    // is passed to Materialized - context.getStateStore(...) on such a store
    // always yields ValueAndTimestamp<V>-wrapped values, never V directly.
    private lateinit var subscriptions: KeyValueStore<String, ValueAndTimestamp<SubscriptionRecord>>

    override fun init(context: ProcessorContext<String, DecodeResult>) {
        this.context = context
        subscriptions = context.getStateStore(subscriptionsStoreName)
    }

    override fun process(record: Record<String, RawLogRecord>) {
        val raw = record.value()
        val eventKey = EventKey.of(raw.network, raw.txHash, raw.logIndex)
        val subscription = findActiveSubscription(raw.network, raw.contractAddress)

        val result = if (subscription != null) {
            decoder.decode(raw, subscription.abiRef)
        } else {
            val removedButKnown = findAnySubscription(raw.network, raw.contractAddress)
            val reason = if (removedButKnown != null) {
                "subscription for network=${raw.network} address=${raw.contractAddress} is ${removedButKnown.status}, not ACTIVE"
            } else {
                "no active subscription found for network=${raw.network} address=${raw.contractAddress}"
            }
            DecodeResult.Failure(
                DecodeFailureRecord(
                    rawLog = raw,
                    abiRef = removedButKnown?.abiRef ?: "<unresolved>",
                    reason = reason,
                    failedAtEpochMillis = clock(),
                ),
            )
        }

        context.forward(Record(eventKey, result, context.currentSystemTimeMs()))
    }

    private fun findActiveSubscription(network: String, address: String): SubscriptionRecord? =
        findAnySubscription(network, address)?.takeIf { it.status == SubscriptionStatus.ACTIVE }

    private fun findAnySubscription(network: String, address: String): SubscriptionRecord? {
        subscriptions.all().use { iter ->
            for (kv in iter) {
                val sub = kv.value.value()
                if (sub.network == network && sub.address.equals(address, ignoreCase = true)) {
                    return sub
                }
            }
        }
        return null
    }

    override fun close() {}
}
