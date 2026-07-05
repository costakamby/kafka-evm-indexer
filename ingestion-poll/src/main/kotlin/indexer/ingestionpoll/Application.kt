package indexer.ingestionpoll

import indexer.ingestionpoll.config.PollAppConfig
import indexer.ingestionpoll.config.PollAppConfigLoader
import indexer.ingestionpoll.progress.KafkaPollProgressStore
import indexer.ingestionpoll.progress.POLL_PROGRESS_TOPIC
import indexer.ingestionpoll.progress.PollProgressRestorer
import indexer.ingestionpoll.poll.ContractPoller
import indexer.ingestionpoll.poll.Topics
import indexer.ingestionpoll.rpc.HttpEthRpcClient
import indexer.ingestionpoll.subscriptions.HttpSubscriptionsReader
import indexer.ingestionpoll.subscriptions.SubscriptionsReader
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties

private val log = LoggerFactory.getLogger("indexer.ingestionpoll.Application")

fun main() {
    val config = PollAppConfigLoader.load()
    val producer: Producer<String, String> = kafkaProducer(config)

    val progressStore = KafkaPollProgressStore(
        producer = producer,
        topic = POLL_PROGRESS_TOPIC,
        initialState = restorePollProgress(config),
    )
    val subscriptionsReader: SubscriptionsReader =
        HttpSubscriptionsReader(httpClient = HttpClient(CIO), baseUrl = config.poll.subscriptionApiBaseUrl)

    runBlocking {
        config.networks.forEach { (network, networkConfig) ->
            val rpcClient = HttpEthRpcClient(
                httpClient = HttpClient(CIO),
                rpcUrl = networkConfig.rpcUrl,
                maxRetries = config.poll.maxRetries,
                initialBackoffMs = config.poll.initialBackoffMs,
                maxBackoffMs = config.poll.maxBackoffMs,
            )
            val poller = ContractPoller(
                rpcClient = rpcClient,
                progressStore = progressStore,
                producer = producer,
                rawLogsTopic = Topics.RAW_LOGS_TOPIC,
                maxBlockRange = config.poll.maxBlockRange,
            )
            launchPollLoop(this, network, config, subscriptionsReader, poller)
        }
    }
}

private fun launchPollLoop(
    scope: CoroutineScope,
    network: String,
    config: PollAppConfig,
    subscriptionsReader: SubscriptionsReader,
    poller: ContractPoller,
) {
    scope.launch(Dispatchers.Default) {
        while (true) {
            try {
                val subscriptions = subscriptionsReader.activeSubscriptions(network)
                poller.pollNetwork(network, subscriptions)
            } catch (e: Exception) {
                log.error("poll cycle failed for network {}, will retry next cycle", network, e)
            }
            delay(config.poll.pollIntervalMs)
        }
    }
}

private fun kafkaProducer(config: PollAppConfig): Producer<String, String> {
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = config.poll.kafkaBootstrapServers
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    props[ProducerConfig.ACKS_CONFIG] = "all"
    return KafkaProducer(props)
}

/** Rebuilds the poll-progress-topic watermark cache once at startup so a restart resumes, not restarts. */
private fun restorePollProgress(config: PollAppConfig): Map<String, Long> {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = config.poll.kafkaBootstrapServers
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[ConsumerConfig.GROUP_ID_CONFIG] = "ingestion-poll-progress-restore"
    KafkaConsumer<String, String>(props).use { consumer ->
        val partitionCount = consumer.partitionsFor(POLL_PROGRESS_TOPIC)?.size ?: 1
        val partitions = (0 until partitionCount).map { TopicPartition(POLL_PROGRESS_TOPIC, it) }
        return PollProgressRestorer.restore(consumer, partitions)
    }
}
