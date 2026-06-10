package com.project.eventlab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.eventlab.dto.order.OrderCreatedData;
import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentProcessedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessedConsumer.class);

    private final ObjectMapper objectMapper;
    private final NotificationService notificationService;

    public PaymentProcessedConsumer(ObjectMapper objectMapper, NotificationService notificationService) {
        this.objectMapper = objectMapper;
        this.notificationService = notificationService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-processed}",
            groupId = "payment-processed-logger"
    )
    public void consume(EventEnvelope<?> event) {
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
    }

}
