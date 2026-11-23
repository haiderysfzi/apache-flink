package com.example.flinkjob.model;

/**
 * A raw event. Written as a Flink "POJO" (public no-arg constructor + public fields) so Flink uses its
 * fast PojoSerializer instead of falling back to Kryo, and so state/savepoints can evolve safely.
 */
public class Event {
    /** Deterministic id (derived from the generator sequence number) -> sinks can de-duplicate replays. */
    public String eventId;
    public String userId;
    public String eventType;
    public double amount;
    /** Event time, epoch millis. */
    public long ts;

    public Event() {}

    public Event(String eventId, String userId, String eventType, double amount, long ts) {
        this.eventId = eventId;
        this.userId = userId;
        this.eventType = eventType;
        this.amount = amount;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" + eventId + ", " + userId + ", " + eventType + ", " + amount + ", " + ts + "}";
    }
}
