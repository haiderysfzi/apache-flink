package com.example.flinkjob.source;

import com.example.flinkjob.model.Event;
import java.util.Random;
import java.util.UUID;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

/**
 * Turns a sequence number into an Event. Everything except {@code ts} is a pure function of the index,
 * so after a failure/restore the replayed records get the SAME eventId -> idempotent sinks
 * (Postgres upsert, ClickHouse ReplacingMergeTree) converge to the same data.
 *
 * Replace this + the source in Job.java with KafkaSource / FileSource / CDC for real data; keep the
 * "deterministic id" idea (e.g. Kafka topic-partition-offset or a business key).
 */
public class EventGenerator implements GeneratorFunction<Long, Event> {
    private static final long serialVersionUID = 1L;
    private static final String[] TYPES = {"view", "click", "purchase", "refund"};

    private final int numUsers;

    public EventGenerator(int numUsers) {
        this.numUsers = numUsers;
    }

    @Override
    public Event map(Long index) {
        Random r = new Random(index);
        String eventId = UUID.nameUUIDFromBytes(("evt-" + index).getBytes()).toString();
        String userId = "user-" + r.nextInt(numUsers);
        String type = TYPES[r.nextInt(TYPES.length)];
        double amount = Math.round(r.nextDouble() * 10000) / 100.0;
        return new Event(eventId, userId, type, amount, System.currentTimeMillis());
    }
}
