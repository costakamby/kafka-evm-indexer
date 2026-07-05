package indexer.integrationtests.fixtures

import org.awaitility.kotlin.await
import org.web3j.abi.FunctionEncoder
import org.web3j.abi.datatypes.Address
import org.web3j.abi.datatypes.Function
import org.web3j.abi.datatypes.generated.Uint256
import org.web3j.crypto.Credentials
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.methods.response.TransactionReceipt
import org.web3j.tx.RawTransactionManager
import java.math.BigInteger
import java.time.Duration

/**
 * Deploys the minimal test ERC20 (design doc section 5.3) and drives
 * transfer/mint calls with known values, including near-2^256 amounts, to
 * exercise the BigInteger-as-string rule end-to-end.
 */
class Erc20Fixture(private val web3j: Web3j, private val credentials: Credentials, chainId: Long) {

    private val txManager = RawTransactionManager(web3j, credentials, chainId)

    lateinit var contractAddress: String
        private set

    fun deploy(initialSupply: BigInteger): String {
        val constructorArgs = FunctionEncoder.encodeConstructor(listOf(Uint256(initialSupply)))
        val data = TestErc20Artifact.bytecode + constructorArgs
        val gasPrice = web3j.ethGasPrice().send().gasPrice

        val response = txManager.sendTransaction(gasPrice, GAS_LIMIT, null, data, BigInteger.ZERO)
        check(response.error == null) { "deploy failed: ${response.error?.message}" }

        val receipt = awaitReceipt(response.transactionHash)
        contractAddress = requireNotNull(receipt.contractAddress) { "deploy did not produce a contract address" }
        return contractAddress
    }

    fun transfer(to: String, value: BigInteger): TransactionReceipt = sendCall("transfer", listOf(Address(to), Uint256(value)))

    fun mint(to: String, value: BigInteger): TransactionReceipt = sendCall("mint", listOf(Address(to), Uint256(value)))

    private fun sendCall(functionName: String, inputs: List<org.web3j.abi.datatypes.Type<*>>): TransactionReceipt {
        val function = Function(functionName, inputs, emptyList())
        val encoded = FunctionEncoder.encode(function)
        val gasPrice = web3j.ethGasPrice().send().gasPrice

        val response = txManager.sendTransaction(gasPrice, GAS_LIMIT, contractAddress, encoded, BigInteger.ZERO)
        check(response.error == null) { "$functionName failed: ${response.error?.message}" }
        return awaitReceipt(response.transactionHash)
    }

    private fun awaitReceipt(txHash: String): TransactionReceipt {
        var receipt: TransactionReceipt? = null
        await.atMost(Duration.ofSeconds(15)).until {
            receipt = web3j.ethGetTransactionReceipt(txHash).send().transactionReceipt.orElse(null)
            receipt != null
        }
        return requireNotNull(receipt)
    }

    private companion object {
        val GAS_LIMIT: BigInteger = BigInteger.valueOf(3_000_000)
    }
}
