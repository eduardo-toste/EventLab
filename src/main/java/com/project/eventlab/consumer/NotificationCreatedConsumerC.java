package com.project.eventlab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.eventlab.dto.notification.NotificationCreatedData;
import com.project.eventlab.event.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationCreatedConsumerC {

    private static final Logger log = LoggerFactory.getLogger(NotificationCreatedConsumerC.class);

    private final ObjectMapper objectMapper;

    public NotificationCreatedConsumerC(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.notification-created}",
            groupId = "notification-created-logger-1"
    )
    public void consume(EventEnvelope<?> event) {
        NotificationCreatedData data = objectMapper.convertValue(event.data(), NotificationCreatedData.class);

        log.info(
                "[NOTIFICATION_CREATED_CONSUMED_C] eventId={} correlationId={} orderId={} channel={} status={}",
                event.eventId(),
                event.correlationId(),
                data.orderId(),
                data.channel(),
                data.status()
        );
    }

}
