package com.project.eventlab.producer;

import com.project.eventlab.dto.notification.NotificationCreatedData;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.service.EventLogService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class NotificationProducer {

    private final String topic;
    private final KafkaTemplate<String, EventEnvelope<NotificationCreatedData>> kafkaTemplate;
    private final EventLogService eventLogService;

    public NotificationProducer(
            @Value("${app.kafka.topics.notification-created}") String topic,
            KafkaTemplate<String, EventEnvelope<NotificationCreatedData>> kafkaTemplate, EventLogService eventLogService
    ) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
        this.eventLogService = eventLogService;
    }

    public void publishNotification(String orderId, EventEnvelope<NotificationCreatedData> event) {
        kafkaTemplate.send(topic, orderId, event);
        eventLogService.savePublished(topic, orderId, event);
    }
}
