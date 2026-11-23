-- Runs once, on first start of an empty data volume. (DB name `flink` must match CLICKHOUSE_DB in .env)
CREATE TABLE IF NOT EXISTS flink.events_raw
(
    event_id    String,
    user_id     LowCardinality(String),
    event_type  LowCardinality(String),
    amount      Float64,
    event_time  DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
-- ReplacingMergeTree collapses rows with the same ORDER BY key during background merges, which makes
-- Flink's at-least-once inserts effectively idempotent. Until a merge happens duplicates are visible:
-- query with FINAL (or argMax/uniq) when exactness matters.
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY event_id;
