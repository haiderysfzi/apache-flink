-- Runs once, on first start of an empty data volume.
CREATE TABLE IF NOT EXISTS user_stats (
    user_id      TEXT             NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    event_count  BIGINT           NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    -- The Flink sink upserts on this key => replays after a restore overwrite instead of duplicating.
    PRIMARY KEY (user_id, window_start)
);

CREATE INDEX IF NOT EXISTS user_stats_window_idx ON user_stats (window_start DESC);
