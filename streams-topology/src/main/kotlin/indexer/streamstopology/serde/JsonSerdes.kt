package indexer.streamstopology.serde

import indexer.schema.json.IndexerJson
import kotlinx.serialization.KSerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

/**
 * Generic kotlinx.serialization-backed Serde, reusing the schema module's
 * shared [IndexerJson.instance] - the same Json config used everywhere else in
 * this project, so wire format is identical between schema round-trip tests
 * and the actual topology.
 */
class JsonSerde<T : Any>(private val serializer: KSerializer<T>) : Serde<T> {
    override fun serializer(): Serializer<T> = Serializer { _, data ->
        IndexerJson.instance.encodeToString(serializer, data).toByteArray(Charsets.UTF_8)
    }

    override fun deserializer(): Deserializer<T> = Deserializer { _, data ->
        IndexerJson.instance.decodeFromString(serializer, String(data, Charsets.UTF_8))
    }
}

fun <T : Any> jsonSerdeOf(serializer: KSerializer<T>): Serde<T> = JsonSerde(serializer)
