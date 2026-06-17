package com.project.eventlab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.eventlab.dto.notification.NotificationCreatedData;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.service.EventLogService;
import com.project.eventlab.mongo.service.IdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationCreatedConsumerA {

    private static final Logger log = LoggerFactory.getLogger(NotificationCreatedConsumerA.class);

    private final ObjectMapper objectMapper;
    private final EventLogService eventLogService;
    private final IdempotencyService idempotencyService;

    @Value("${app.kafka.topics.notification-created}")
    private String topic;

    public NotificationCreatedConsumerA(ObjectMapper objectMapper, EventLogService eventLogService, IdempotencyService idempotencyService) {
        this.objectMapper = objectMapper;
        this.eventLogService = eventLogService;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.notification-created}",
            groupId = "notification-created-logger"
    )
    public void consume(EventEnvelope<?> event) {
        startProcessingEvent(event);

        try {
            NotificationCreatedData data = objectMapper.convertValue(event.data(), NotificationCreatedData.class);

            log.info(
                    "[NOTIFICATION_CREATED_CONSUMED] eventId={} correlationId={} orderId={} channel={} status={}",
                    event.eventId(),
                    event.correlationId(),
                    data.orderId(),
                    data.channel(),
                    data.status()
            );

            eventLogService.saveConsumed(topic, "notification-created-logger-a", event);
            idempotencyService.markProcessed(event.eventId(), "notification-created-logger-a");
        } catch (Exception ex) {
            idempotencyService.markFailed(event.eventId(), "notification-created-logger", ex.getMessage());
            throw ex;
        }
    }

    private void startProcessingEvent(EventEnvelope<?> event) {
        boolean acquired = idempotencyService.tryStartProcessing(
                event.eventId(),
                "notification-created-logger",
                event.correlationId(),
                event.eventType()
        );

        if (!acquired) {
            log.info(
                    "[NOTIFICATION_CREATED_DUPLICATE] eventId={} correlationId={} consumerName={}",
                    event.eventId(),
                    event.correlationId(),
                    "notification-created-logger"
            );
        }
    }

}
