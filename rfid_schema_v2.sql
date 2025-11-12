-- rfid_schema_v2.sql
-- Safe to run multiple times. This script:
-- 1) Creates schema/table/indexes/hypertable
-- 2) Enables compression + retention policies
-- 3) Creates a continuous aggregate WITH NO DATA (so it can run in any context)
-- 4) Adds a refresh policy (scheduler will populate it)

-- If TimescaleDB extension is not enabled in this DB:
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS rfid;

CREATE TABLE IF NOT EXISTS rfid.reads (
  ts           timestamptz NOT NULL DEFAULT now(),
  reader_id    text        NOT NULL,
  antenna      smallint    NOT NULL,
  epc          bytea       NOT NULL,
  pc           smallint,
  rssi_dbm     real,
  freq_khz     integer,
  phase_deg    smallint,
  raw_frame    bytea,
  src          text,
  epc_hex      text GENERATED ALWAYS AS (encode(epc,'hex')) STORED
);

-- Convert to hypertable (idempotent)
SELECT create_hypertable('rfid.reads','ts', if_not_exists => TRUE);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_reads_epc_ts ON rfid.reads (epc_hex, ts DESC);
CREATE INDEX IF NOT EXISTS idx_reads_reader_ant_ts ON rfid.reads (reader_id, antenna, ts DESC);

-- Compression & retention
ALTER TABLE rfid.reads SET (timescaledb.compress);
SELECT add_compression_policy('rfid.reads', INTERVAL '7 days');
SELECT add_retention_policy('rfid.reads',    INTERVAL '365 days');

-- Continuous aggregate (WITH NO DATA to allow running outside/inside transaction blocks)
CREATE MATERIALIZED VIEW IF NOT EXISTS rfid.tag_presence_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', ts) AS bucket,
       reader_id, antenna, epc_hex,
       min(ts) AS first_seen,
       max(ts) AS last_seen,
       count(*) AS reads,
       avg(rssi_dbm) AS avg_rssi_dbm
FROM rfid.reads
GROUP BY 1,2,3,4
WITH NO DATA;

-- Continuous aggregate refresh policy
SELECT add_continuous_aggregate_policy(
  'rfid.tag_presence_1m',
  start_offset      => INTERVAL '1 day',
  end_offset        => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute'
);

-- Optional: manual backfill for the last 24h (uncomment if you already have data)
-- CALL refresh_continuous_aggregate('rfid.tag_presence_1m', now() - INTERVAL '1 day', now());
