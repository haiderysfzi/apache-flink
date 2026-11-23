package com.example.flinkjob.config;

import java.io.Serializable;

/**
 * Runtime configuration, read from environment variables on the machine that SUBMITS the job
 * (the jobmanager container when using scripts/). Values are captured when the job graph is built
 * and shipped to the TaskManagers inside the serialized operators, so they do not need to be set
 * on TaskManagers (though docker-compose sets both for convenience).
 */
public final class AppConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String pgUrl;
    public final String pgUser;
    public final String pgPassword;
    public final String chUrl;
    public final String chUser;
    public final String chPassword;

    public final int eventsPerSecond;
    public final int numUsers;
    public final int windowSeconds;
    public final long checkpointIntervalMs;
    public final int jdbcBatchSize;
    public final long jdbcBatchIntervalMs;
    public final int jdbcMaxRetries;

    private AppConfig() {
        pgUrl = env("PG_URL", "jdbc:postgresql://localhost:5432/flink");
        pgUser = env("PG_USER", "flink");
        pgPassword = env("PG_PASSWORD", "flink");
        chUrl = env("CH_URL", "jdbc:clickhouse://localhost:8123/flink");
        chUser = env("CH_USER", "flink");
        chPassword = env("CH_PASSWORD", "flink");

        eventsPerSecond = Integer.parseInt(env("EVENTS_PER_SECOND", "200"));
        numUsers = Integer.parseInt(env("NUM_USERS", "50"));
        windowSeconds = Integer.parseInt(env("WINDOW_SECONDS", "10"));
        checkpointIntervalMs = Long.parseLong(env("CHECKPOINT_INTERVAL_MS", "10000"));
        jdbcBatchSize = Integer.parseInt(env("JDBC_BATCH_SIZE", "1000"));
        jdbcBatchIntervalMs = Long.parseLong(env("JDBC_BATCH_INTERVAL_MS", "1000"));
        jdbcMaxRetries = Integer.parseInt(env("JDBC_MAX_RETRIES", "3"));
    }

    public static AppConfig fromEnv() {
        return new AppConfig();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
