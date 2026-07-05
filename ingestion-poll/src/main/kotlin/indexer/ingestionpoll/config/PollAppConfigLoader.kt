package indexer.ingestionpoll.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource

object PollAppConfigLoader {
    fun load(): PollAppConfig =
        ConfigLoaderBuilder.default()
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<PollAppConfig>()
}
