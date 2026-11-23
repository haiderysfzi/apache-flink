package com.example.flinkjob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.example.flinkjob.model.Event;
import com.example.flinkjob.source.EventGenerator;
import org.junit.jupiter.api.Test;

class EventGeneratorTest {
    @Test
    void sameIndexGivesSameIdentityAndPayload() {
        EventGenerator g = new EventGenerator(50);
        Event a = g.map(42L);
        Event b = g.map(42L);
        assertEquals(a.eventId, b.eventId);
        assertEquals(a.userId, b.userId);
        assertEquals(a.eventType, b.eventType);
        assertEquals(a.amount, b.amount);
    }

    @Test
    void differentIndexGivesDifferentId() {
        EventGenerator g = new EventGenerator(50);
        assertNotEquals(g.map(1L).eventId, g.map(2L).eventId);
    }
}
