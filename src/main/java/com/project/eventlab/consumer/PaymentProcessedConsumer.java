package com.project.eventlab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.enums.ProcessingStartDecision;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.service.EventLogService;
import com.project.eventlab.mongo.service.IdempotencyService;
import com.project.eventlab.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentProcessedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessedConsumer.class);
    private static final String CONSUMER_NAME = "payment-processed-logger";

    private final ObjectMapper objectMapper;
    private final NotificationService notificationService;
    private final EventLogService eventLogService;
    private final IdempotencyService idempotencyService;

    @Value("${app.kafka.topics.payment-processed}")
    private String topic;

    public PaymentProcessedConsumer(ObjectMapper objectMapper, NotificationService notificationService, EventLogService eventLogService, IdempotencyService idempotencyService) {
        this.objectMapper = objectMapper;
        this.notificationService = notificationService;
        this.eventLogService = eventLogService;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-processed}",
            groupId = CONSUMER_NAME
    )
    public void consume(EventEnvelope<?> event) {
        if (!startProcessingEvent(event)) {
            return;
        }

        try {
            PaymentProcessedData data = objectMapper.convertValue(event.data(), PaymentProcessedData.class);
            EventEnvelope<PaymentProcessedData> paymentProcessedEvent = new EventEnvelope<>(
                    event.eventId(),
                    event.correlationId(),
                    event.eventType(),
                    event.version(),
                    event.occurredAt(),
                    data
            );

            log.info(
                    "[PAYMENT_PROCESSED_CONSUMED] eventId={} correlationId={} orderId={} paymentId={} total={}",
                    event.eventId(),
                    event.correlationId(),
                    data.orderId(),
                    data.paymentId(),
                    data.amount()
            );

            notificationService.sendNotification(paymentProcessedEvent);
            idempotencyService.markProcessed(event.eventId(), CONSUMER_NAME);
            eventLogService.saveConsumed(topic, CONSUMER_NAME, event);
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
                        "[PAYMENT_PROCESSED_RETRYING_FAILED] eventId={} correlationId={} consumerName={}",
                        event.eventId(),
                        event.correlationId(),
                        CONSUMER_NAME
                );
                return true;
            case ALREADY_PROCESSED:
                log.info(
                        "[PAYMENT_PROCESSED_DUPLICATE] eventId={} correlationId={} consumerName={}",
                        event.eventId(),
                        event.correlationId(),
                        CONSUMER_NAME
                );
                return false;
            case ALREADY_PROCESSING:
                log.info(
                        "[PAYMENT_PROCESSED_ALREADY_PROCESSING] eventId={} correlationId={} consumerName={}",
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
