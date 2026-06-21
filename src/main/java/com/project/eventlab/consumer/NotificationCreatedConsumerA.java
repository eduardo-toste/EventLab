package com.project.eventlab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.eventlab.dto.notification.NotificationCreatedData;
import com.project.eventlab.enums.ProcessingStartDecision;
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
    private static final String CONSUMER_NAME = "notification-created-logger-a";

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
        if (!startProcessingEvent(event)) {
            return;
        }

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

            eventLogService.saveConsumed(topic, CONSUMER_NAME, event);
            idempotencyService.markProcessed(event.eventId(), CONSUMER_NAME);
        } catch (Exception ex) {
            idempotencyService.markFailed(event.eventId(), CONSUMER_NAME, ex.getMessage());
            throw ex;
        }
    }

    private boolean startProcessingEvent(EventEnvelope<?> event) {
        ProcessingStartDecision decision = idempotencyService.tryStartProcessing(
                event.eventId(),
                CONSUMER_NAME,
                event.correlationId(),
                event.eventType()
        );

        switch (decision) {
            case STARTED:
                return true;
            case RETRYING_FAILED:
                log.info(
                        "[NOTIFICATION_CREATED_RETRYING_FAILED] eventId={} correlationId={} consumerName={}",
                        event.eventId(),
                        event.correlationId(),
                        CONSUMER_NAME
                );
                return true;
            case ALREADY_PROCESSED:
                log.info(
                        "[NOTIFICATION_CREATED_DUPLICATE] eventId={} correlationId={} consumerName={}",
                        event.eventId(),
                        event.correlationId(),
                        CONSUMER_NAME
                );
                return false;
            case ALREADY_PROCESSING:
                log.info(
                        "[NOTIFICATION_CREATED_ALREADY_PROCESSING] eventId={} correlationId={} consumerName={}",
                        event.eventId(),
                        event.correlationId(),
                        CONSUMER_NAME
                );
                return false;
            default:
                throw new IllegalStateException("Unsupported processing start decision: " + decision);
        }
    }

}
