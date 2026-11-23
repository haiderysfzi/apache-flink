package com.example.flinkjob.sink;

import com.example.flinkjob.config.AppConfig;
import com.example.flinkjob.model.Event;
import com.example.flinkjob.model.UserStat;
import java.sql.Timestamp;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Both sinks are batched JDBC sinks with AT-LEAST-ONCE delivery: the buffer is flushed on every
 * checkpoint, so after a restore some records are written twice. That is harmless because both
 * targets are idempotent on a stable key:
 *
 *  - Postgres: INSERT ... ON CONFLICT (user_id, window_start) DO UPDATE  (upsert)
 *  - ClickHouse: ReplacingMergeTree ORDER BY event_id                    (dedupe on merge / FINAL)
 *
 * Every sink gets a .uid()/.name() in Job.java so savepoints keep matching operators across redeploys.
 */
public final class Sinks {
    private Sinks() {}

    private static final String PG_UPSERT =
            "INSERT INTO user_stats (user_id, window_start, window_end, event_count, total_amount) "
                    + "VALUES (?, ?, ?, ?, ?) "
                    + "ON CONFLICT (user_id, window_start) DO UPDATE SET "
                    + "window_end = EXCLUDED.window_end, "
                    + "event_count = EXCLUDED.event_count, "
                    + "total_amount = EXCLUDED.total_amount, "
                    + "updated_at = now()";

    private static final String CH_INSERT =
            "INSERT INTO events_raw (event_id, user_id, event_type, amount, event_time) VALUES (?, ?, ?, ?, ?)";

    public static SinkFunction<UserStat> postgresUserStats(AppConfig c) {
        return JdbcSink.sink(
                PG_UPSERT,
                (ps, s) -> {
                    ps.setString(1, s.userId);
                    ps.setTimestamp(2, new Timestamp(s.windowStart));
                    ps.setTimestamp(3, new Timestamp(s.windowEnd));
                    ps.setLong(4, s.eventCount);
                    ps.setDouble(5, s.totalAmount);
                },
                execution(c),
                connection(c.pgUrl, "org.postgresql.Driver", c.pgUser, c.pgPassword));
    }

    public static SinkFunction<Event> clickhouseEvents(AppConfig c) {
        return JdbcSink.sink(
                CH_INSERT,
                (ps, e) -> {
                    ps.setString(1, e.eventId);
                    ps.setString(2, e.userId);
                    ps.setString(3, e.eventType);
                    ps.setDouble(4, e.amount);
                    ps.setTimestamp(5, new Timestamp(e.ts));
                },
                execution(c),
                connection(c.chUrl, "com.clickhouse.jdbc.ClickHouseDriver", c.chUser, c.chPassword));
    }

    private static JdbcExecutionOptions execution(AppConfig c) {
        return JdbcExecutionOptions.builder()
                .withBatchSize(c.jdbcBatchSize)
                .withBatchIntervalMs(c.jdbcBatchIntervalMs)
                .withMaxRetries(c.jdbcMaxRetries)
                .build();
    }

    private static JdbcConnectionOptions connection(String url, String driver, String user, String password) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName(driver)
                .withUsername(user)
                .withPassword(password)
                .withConnectionCheckTimeoutSeconds(10) // bounded: never wait forever on a dead DB
                .build();
    }
}
