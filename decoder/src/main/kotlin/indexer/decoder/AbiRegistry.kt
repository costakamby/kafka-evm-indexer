package indexer.decoder

import java.util.concurrent.ConcurrentHashMap

/**
 * Resolves an `abiRef` string (as supplied to `POST /subscriptions`) to a real,
 * parseable ABI. Resolution mechanism: an abiRef names a checked-in classpath
 * resource `/abis/<abiRef>.json`. A ref with no such resource is unknown; a
 * resource that exists but fails to parse is malformed. Both cases surface as a
 * failed resolution so the API layer can reject with a 400 BEFORE anything is
 * produced to subscriptions-topic (acceptance criterion 4.1).
 *
 * A [seed] map is accepted for tests and for injecting ABIs from a source other
 * than the classpath without changing the resolution contract.
 */
class AbiRegistry(
    private val seed: Map<String, String> = emptyMap(),
    private val resourceLoader: (String) -> String? = { ref ->
        AbiRegistry::class.java.getResourceAsStream("/abis/$ref.json")
            ?.bufferedReader()
            ?.use { it.readText() }
    },
) {
    private val cache = ConcurrentHashMap<String, ParsedAbi>()

    /** True if [abiRef] resolves to a real, parseable ABI (used by the API validation path). */
    fun isValid(abiRef: String): Boolean = try {
        resolve(abiRef)
        true
    } catch (_: Exception) {
        false
    }

    /**
     * Resolves and parses the ABI for [abiRef].
     * @throws AbiParseException if the ref is unknown or its ABI JSON is malformed.
     */
    fun resolve(abiRef: String): ParsedAbi = cache.getOrPut(abiRef) {
        val abiJson = seed[abiRef]
            ?: resourceLoader(abiRef)
            ?: throw AbiParseException("unknown abiRef '$abiRef' (no /abis/$abiRef.json on classpath)")
        AbiParser.parse(abiRef, abiJson)
    }
}
