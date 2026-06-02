package com.project.eventlab.event;

import java.time.LocalDateTime;

public record EventEnvelope<T>(

        String eventId,
        String correlationId,
        String eventType,
        String version,
        LocalDateTime occurredAt,
        T data

) {
}
