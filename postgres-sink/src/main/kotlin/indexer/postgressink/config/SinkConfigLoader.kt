package indexer.postgressink.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object SinkConfigLoader {
    fun load(): SinkConfig =
        ConfigLoaderBuilder.default()
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<SinkConfig>()
}
