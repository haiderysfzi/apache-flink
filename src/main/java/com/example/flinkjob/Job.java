package com.example.flinkjob;

import com.example.flinkjob.config.AppConfig;
import com.example.flinkjob.model.Event;
import com.example.flinkjob.model.UserStat;
import com.example.flinkjob.sink.Sinks;
import com.example.flinkjob.source.EventGenerator;
import com.example.flinkjob.transform.UserStatAggregation;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

/**
 * Pipeline:
 *
 *   generator source ──┬──> ClickHouse  events_raw   (every raw event)
 *                      │
 *                      └──> keyBy(userId) -> 10s tumbling window -> Postgres user_stats (upsert)
 *
 * The JOB_NAME is what scripts/ look for when finding the running job, so keep it stable.
 */
public class Job {
    public static final String JOB_NAME = "flink-template-job";

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.fromEnv();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- fault tolerance -------------------------------------------------------------
        env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(cfg.checkpointIntervalMs / 2);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(20, Time.seconds(10)));
        // Retaining checkpoints on cancel is set cluster-side (docker-compose FLINK_PROPERTIES).

        // --- source ----------------------------------------------------------------------
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new EventGenerator(cfg.numUsers),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(cfg.eventsPerSecond),
                TypeInformation.of(Event.class));

        WatermarkStrategy<Event> watermarks = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((e, recordTs) -> e.ts)
                .withIdleness(Duration.ofSeconds(10));

        DataStream<Event> events = env.fromSource(source, watermarks, "event-generator").uid("source-events");

        // --- branch 1: raw events -> ClickHouse ------------------------------------------
        events.addSink(Sinks.clickhouseEvents(cfg)).uid("sink-clickhouse-events").name("ClickHouse events_raw");

        // --- branch 2: windowed aggregate -> Postgres ------------------------------------
        events.keyBy(e -> e.userId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(cfg.windowSeconds)))
                .aggregate(new UserStatAggregation.Agg(), new UserStatAggregation.Emit())
                .uid("window-user-stats").name("user stats per window")
                .addSink(Sinks.postgresUserStats(cfg)).uid("sink-postgres-user-stats").name("Postgres user_stats");

        env.execute(JOB_NAME);
    }
}
