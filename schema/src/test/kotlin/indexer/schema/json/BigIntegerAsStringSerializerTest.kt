package indexer.schema.json

import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import java.math.BigInteger

/**
 * Hard rule (design doc decision 10): on-chain integers must never be
 * serialized as JSON numbers, regardless of magnitude. Verified for both a
 * trivially small value and a near-2^256 value, in both directions.
 */
class BigIntegerAsStringSerializerTest {

    @Serializable
    private data class Wrapper(
        @Serializable(with = BigIntegerAsStringSerializer::class)
        val value: BigInteger,
    )

    private val json = Json

    @Test
    fun `tiny value is encoded as a JSON string, not a JSON number`() {
        val encoded = json.encodeToString(Wrapper.serializer(), Wrapper(BigInteger.valueOf(5)))

        encoded shouldBe """{"value":"5"}"""
    }

    @Test
    fun `near 2 to the 256 value is encoded as a JSON string, not a JSON number`() {
        val near2to256 = BigInteger.TWO.pow(256).subtract(BigInteger.ONE)

        val encoded = json.encodeToString(Wrapper.serializer(), Wrapper(near2to256))

        encoded shouldBe """{"value":"$near2to256"}"""
    }

    @Test
    fun `tiny value round trips exactly through decode`() {
        val original = BigInteger.valueOf(5)

        val decoded = json.decodeFromString(Wrapper.serializer(), json.encodeToString(Wrapper.serializer(), Wrapper(original)))

        decoded.value shouldBe original
    }

    @Test
    fun `near 2 to the 256 value round trips exactly through decode with full precision`() {
        val original = BigInteger.TWO.pow(256).subtract(BigInteger.ONE)

        val decoded = json.decodeFromString(Wrapper.serializer(), json.encodeToString(Wrapper.serializer(), Wrapper(original)))

        decoded.value shouldBe original
    }

    @Test
    fun `a JSON payload with a raw numeric literal is rejected, not silently accepted`() {
        // Guards against a producer accidentally emitting a bare number instead of a string.
        val rawNumberPayload = """{"value":5}"""

        try {
            json.decodeFromString(Wrapper.serializer(), rawNumberPayload)
            throw AssertionError("expected decoding a raw JSON number to fail")
        } catch (e: Exception) {
            // expected: raw numeric literals are not a valid on-chain integer encoding
        }
    }
}
