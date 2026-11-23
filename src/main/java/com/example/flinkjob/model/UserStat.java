package com.example.flinkjob.model;

/** Per-user aggregate over one tumbling event-time window. */
public class UserStat {
    public String userId;
    /** Window bounds, epoch millis. windowStart is part of the Postgres primary key. */
    public long windowStart;
    public long windowEnd;
    public long eventCount;
    public double totalAmount;

    public UserStat() {}

    public UserStat(String userId, long windowStart, long windowEnd, long eventCount, double totalAmount) {
        this.userId = userId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.eventCount = eventCount;
        this.totalAmount = totalAmount;
    }
}
