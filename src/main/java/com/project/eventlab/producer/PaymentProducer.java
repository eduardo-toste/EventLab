package com.project.eventlab.producer;

import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.service.EventLogService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentProducer {

    private final String topic;
    private final KafkaTemplate<String, EventEnvelope<PaymentProcessedData>> kafkaTemplate;
    private final EventLogService eventLogService;

    public PaymentProducer(
            @Value("${app.kafka.topics.payment-processed}") String topic,
            KafkaTemplate<String, EventEnvelope<PaymentProcessedData>> kafkaTemplate, EventLogService eventLogService
    ) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
        this.eventLogService = eventLogService;
    }

    public void publishProcessedPayment(String orderId, EventEnvelope<PaymentProcessedData> event) {
        kafkaTemplate.send(topic, orderId, event);
        eventLogService.savePublished(topic, orderId, event);
    }

}
