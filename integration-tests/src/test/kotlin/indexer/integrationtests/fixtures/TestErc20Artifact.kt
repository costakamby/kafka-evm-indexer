package indexer.integrationtests.fixtures

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * ABI+bytecode for the minimal test ERC20, compiled once via Foundry and
 * checked in at contracts/TestERC20.json - see integration-tests/contracts/README.md
 * to regenerate after an edit.
 */
object TestErc20Artifact {
    val bytecode: String by lazy {
        val text = requireNotNull(TestErc20Artifact::class.java.getResourceAsStream("/contracts/TestERC20.json")) {
            "missing contracts/TestERC20.json test resource - see integration-tests/contracts/README.md"
        }.bufferedReader().readText()
        Json.parseToJsonElement(text).jsonObject.getValue("bytecode").jsonPrimitive.content
    }
}
