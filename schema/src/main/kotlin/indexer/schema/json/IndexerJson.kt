package indexer.schema.json

import kotlinx.serialization.json.Json

/** Shared Json configuration for every topic contract in the schema module. */
object IndexerJson {
    val instance: Json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }
}
