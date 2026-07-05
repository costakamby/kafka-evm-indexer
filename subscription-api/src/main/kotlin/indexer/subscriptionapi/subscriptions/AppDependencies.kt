package indexer.subscriptionapi.subscriptions

import indexer.decoder.AbiRegistry

/** Everything the subscription REST routes need, injectable for testing with fakes. */
data class AppDependencies(
    val subscriptionWriter: SubscriptionWriter,
    val subscriptionReader: SubscriptionReader,
    val abiRegistry: AbiRegistry,
)
