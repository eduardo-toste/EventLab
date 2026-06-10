package com.project.eventlab.producer;

import com.project.eventlab.dto.notification.NotificationCreatedData;
import com.project.eventlab.event.EventEnvelope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class NotificationProducer {

    private String topic;
    private final KafkaTemplate<String, EventEnvelope<NotificationCreatedData>> kafkaTemplate;

    public NotificationProducer(
            @Value("${app.kafka.topics.notification-created}") String topic,
            KafkaTemplate<String, EventEnvelope<NotificationCreatedData>> kafkaTemplate
    ) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishNotification(String orderId, EventEnvelope<NotificationCreatedData> event) {
        kafkaTemplate.send(topic, orderId, event);
    }
}
