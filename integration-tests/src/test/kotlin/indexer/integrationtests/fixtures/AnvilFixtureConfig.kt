package indexer.integrationtests.fixtures

/**
 * Pinned fork config (design doc section 5.3) - never fork "latest". Same
 * defaults as .env.example so local dev and tests fork the same state.
 */
object AnvilFixtureConfig {
    val forkUrl: String = System.getenv("ANVIL_FORK_URL") ?: "https://eth-mainnet.public.blastapi.io"
    val forkBlock: Long = System.getenv("ANVIL_FORK_BLOCK")?.toLong() ?: 18_000_000L
}
