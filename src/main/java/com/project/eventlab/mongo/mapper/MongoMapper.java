package com.project.eventlab.mongo.mapper;

import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.document.EventLogDocument;
import org.springframework.stereotype.Component;

@Component
public class MongoMapper {

    public EventLogDocument toPublishedDocument(String topic, String key, EventEnvelope<?> event) {
        return new EventLogDocument(
                event.eventId(),
                event.correlationId(),
                event.eventType(),
                topic,
                key,
                "PUBLISHED",
                null,
                event.data(),
                event.occurredAt()
        );
    }

    public EventLogDocument toConsumedDocument(String topic, String consumerName, EventEnvelope<?> event) {
        return new EventLogDocument(
                event.eventId(),
                event.correlationId(),
                event.eventType(),
                topic,
                null,
                "PUBLISHED",
                consumerName,
                event.data(),
                event.occurredAt()
        );
    }

}
