package indexer.postgressink.db

import indexer.schema.ConfirmationStatus
import indexer.schema.DecodedEventEnvelope
import indexer.schema.IngestionSource
import indexer.schema.json.IndexerJson
import kotlinx.serialization.json.JsonObject
import org.flywaydb.core.Flyway
import java.sql.PreparedStatement
import java.sql.ResultSet
import javax.sql.DataSource

/**
 * The postgres-sink module's only write path into Postgres (design decision
 * 1: Postgres is a downstream read-model, populated by exactly this
 * consumer - never queried or written by the Streams app itself).
 *
 * All mutation goes through [ConfirmedEventUpsert]'s upsert-on-
 * (network, tx_hash, log_index) SQL, which is what makes [upsert]
 * idempotent and what makes an INVALIDATED correction overwrite the
 * existing row in place (design doc 4.7).
 */
class ConfirmedEventRepository(private val dataSource: DataSource) {

    /** Applies checked-in Flyway migrations from db/migration on the classpath. */
    fun migrate() {
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .load()
            .migrate()
    }

    fun upsert(envelope: DecodedEventEnvelope) {
        dataSource.connection.use { conn ->
            conn.prepareStatement(ConfirmedEventUpsert.SQL).use { stmt ->
                bind(stmt, ConfirmedEventUpsert.paramsFor(envelope))
                stmt.executeUpdate()
            }
        }
    }

    fun findByKey(network: String, txHash: String, logIndex: Long): ConfirmedEventRow? {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT network, tx_hash, log_index, event_name, signature_hash,
                       contract_address, block_number, status, source, decoded_fields
                FROM confirmed_events
                WHERE network = ? AND tx_hash = ? AND log_index = ?
                """.trimIndent(),
            ).use { stmt ->
                stmt.setString(1, network)
                stmt.setString(2, txHash)
                stmt.setLong(3, logIndex)
                stmt.executeQuery().use { rs ->
                    return if (rs.next()) rs.toConfirmedEventRow() else null
                }
            }
        }
    }

    /** All rows, ordered deterministically - used by tests to compare end states. */
    fun snapshotAll(): List<ConfirmedEventRow> {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                """
                SELECT network, tx_hash, log_index, event_name, signature_hash,
                       contract_address, block_number, status, source, decoded_fields
                FROM confirmed_events
                ORDER BY network, tx_hash, log_index
                """.trimIndent(),
            ).use { stmt ->
                stmt.executeQuery().use { rs ->
                    val rows = mutableListOf<ConfirmedEventRow>()
                    while (rs.next()) rows.add(rs.toConfirmedEventRow())
                    return rows
                }
            }
        }
    }

    fun countAll(): Long = scalarLong("SELECT COUNT(*) FROM confirmed_events")

    fun countForKey(network: String, txHash: String, logIndex: Long): Long {
        dataSource.connection.use { conn ->
            conn.prepareStatement(
                "SELECT COUNT(*) FROM confirmed_events WHERE network = ? AND tx_hash = ? AND log_index = ?",
            ).use { stmt ->
                stmt.setString(1, network)
                stmt.setString(2, txHash)
                stmt.setLong(3, logIndex)
                stmt.executeQuery().use { rs ->
                    rs.next()
                    return rs.getLong(1)
                }
            }
        }
    }

    /** Test-only helper: empties the table so each test starts from an empty Postgres. */
    fun truncate() {
        dataSource.connection.use { conn ->
            conn.prepareStatement("TRUNCATE TABLE confirmed_events").use { it.executeUpdate() }
        }
    }

    private fun scalarLong(sql: String): Long {
        dataSource.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    return rs.getLong(1)
                }
            }
        }
    }

    private fun bind(stmt: PreparedStatement, params: List<Any?>) {
        params.forEachIndexed { index, value ->
            val position = index + 1
            when (value) {
                is String -> stmt.setString(position, value)
                is Long -> stmt.setLong(position, value)
                is Int -> stmt.setInt(position, value)
                null -> stmt.setObject(position, null)
                else -> stmt.setObject(position, value)
            }
        }
    }

    private fun ResultSet.toConfirmedEventRow(): ConfirmedEventRow = ConfirmedEventRow(
        network = getString("network"),
        txHash = getString("tx_hash"),
        logIndex = getLong("log_index"),
        eventName = getString("event_name"),
        signatureHash = getString("signature_hash"),
        contractAddress = getString("contract_address"),
        blockNumber = getLong("block_number"),
        status = ConfirmationStatus.valueOf(getString("status")),
        source = IngestionSource.valueOf(getString("source")),
        decodedFields = IndexerJson.instance.parseToJsonElement(getString("decoded_fields")).let {
            it as? JsonObject ?: error("decoded_fields column did not contain a JSON object")
        },
    )
}
