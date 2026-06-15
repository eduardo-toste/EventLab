package com.project.eventlab.mongo.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "event_logs")
public class EventLogDocument {

    @Id
    private String id;
    private String eventId;
    private String correlationId;
    private String eventType;
    private String topic;
    private String key;
    private String direction;
    private String consumerName;
    private Object payload;
    private LocalDateTime createdAt;

    public EventLogDocument(
            String eventId,
            String correlationId,
            String eventType,
            String topic,
            String key,
            String direction,
            String consumerName,
            Object payload,
            LocalDateTime createdAt
    ) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.eventType = eventType;
        this.topic = topic;
        this.key = key;
        this.direction = direction;
        this.consumerName = consumerName;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    protected EventLogDocument() {
    }

}