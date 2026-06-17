package com.project.eventlab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.eventlab.dto.order.OrderCreatedData;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.service.EventLogService;
import com.project.eventlab.mongo.service.IdempotencyService;
import com.project.eventlab.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CreatedOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(CreatedOrderConsumer.class);
    private static final String CONSUMER_NAME = "order-created-logger";

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;
    private final EventLogService eventLogService;
    private final IdempotencyService idempotencyService;

    @Value("${app.kafka.topics.order-created}")
    private String topic;

    public CreatedOrderConsumer(ObjectMapper objectMapper, PaymentService paymentService, EventLogService eventLogService, IdempotencyService idempotencyService) {
        this.objectMapper = objectMapper;
        this.paymentService = paymentService;
        this.eventLogService = eventLogService;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = CONSUMER_NAME
    )
    public void consume(EventEnvelope<?> event) {
        if (!startProcessingEvent(event)) {
            return;
        }

        try {
            OrderCreatedData data = objectMapper.convertValue(event.data(), OrderCreatedData.class);
            EventEnvelope<OrderCreatedData> orderCreatedEvent = new EventEnvelope<>(
                    event.eventId(),
                    event.correlationId(),
                    event.eventType(),
                    event.version(),
                    event.occurredAt(),
                    data
            );

            log.info(
                    "[ORDER_CREATED_CONSUMED] eventId={} correlationId={} orderId={} total={}",
                    event.eventId(),
                    event.correlationId(),
                    data.orderId(),
                    data.total()
            );

            paymentService.processPayment(orderCreatedEvent);
            idempotencyService.markProcessed(event.eventId(), CONSUMER_NAME);
            eventLogService.saveConsumed(topic, CONSUMER_NAME, event);
        } catch (Exception ex) {
            idempotencyService.markFailed(event.eventId(), CONSUMER_NAME, ex.getMessage());
            throw ex;
        }

    }

    private boolean startProcessingEvent(EventEnvelope<?> event) {
        boolean acquired = idempotencyService.tryStartProcessing(
                event.eventId(),
                CONSUMER_NAME,
                event.correlationId(),
                event.eventType()
        );

        if (!acquired) {
            log.info(
                    "[ORDER_CREATED_DUPLICATE] eventId={} correlationId={} consumerName={}",
                    event.eventId(),
                    event.correlationId(),
                    CONSUMER_NAME
            );
            return false;
        }

        return true;
    }

}
