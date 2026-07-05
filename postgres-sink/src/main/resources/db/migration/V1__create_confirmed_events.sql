-- confirmed_events: read-model materialization of confirmed-events-topic
-- (design doc section 4.7). Fixed envelope fields are typed columns for
-- query/indexing; decodedFields is an open, ABI-dependent JsonObject
-- (schema module design) so it is stored as JSONB rather than normalized.
--
-- The UNIQUE constraint on (network, tx_hash, log_index) is what makes the
-- sink consumer's upsert idempotent: replaying confirmed-events-topic from
-- offset 0 re-applies the same rows instead of duplicating them, and an
-- INVALIDATED correction message updates the existing row's status in
-- place instead of leaving a stale CONFIRMED row behind.
CREATE TABLE confirmed_events (
    id                BIGSERIAL PRIMARY KEY,
    network           VARCHAR(64)  NOT NULL,
    tx_hash           VARCHAR(128) NOT NULL,
    log_index         BIGINT       NOT NULL,
    event_name        VARCHAR(255) NOT NULL,
    signature_hash    VARCHAR(128) NOT NULL,
    contract_address  VARCHAR(128) NOT NULL,
    block_number      BIGINT       NOT NULL,
    status            VARCHAR(32)  NOT NULL,
    source            VARCHAR(16)  NOT NULL,
    decoded_fields    JSONB        NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_confirmed_events_network_tx_log UNIQUE (network, tx_hash, log_index)
);

-- Query patterns: "latest events for a network" and "events by status"
-- (e.g. finding stale UNCONFIRMED rows), plus a GIN index so downstream
-- consumers can query into the open decodedFields blob without a full scan.
CREATE INDEX idx_confirmed_events_network_block ON confirmed_events (network, block_number);
CREATE INDEX idx_confirmed_events_status ON confirmed_events (status);
CREATE INDEX idx_confirmed_events_decoded_fields ON confirmed_events USING GIN (decoded_fields);
