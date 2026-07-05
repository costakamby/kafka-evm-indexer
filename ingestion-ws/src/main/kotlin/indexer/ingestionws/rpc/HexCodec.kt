package indexer.ingestionws.rpc

/**
 * Ethereum JSON-RPC encodes block numbers / log indices as 0x-prefixed hex
 * strings, never JSON numbers. Pure conversion helpers, no IO.
 */
object HexCodec {
    fun toLong(hex: String): Long = java.lang.Long.decode(hex)

    fun toHex(value: Long): String = "0x" + java.lang.Long.toHexString(value)
}
