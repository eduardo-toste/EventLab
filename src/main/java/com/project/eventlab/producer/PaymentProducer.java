package com.project.eventlab.producer;

import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.event.EventEnvelope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentProducer {

    public final String topic;
    public final KafkaTemplate<String, EventEnvelope<PaymentProcessedData>> kafkaTemplate;

    public PaymentProducer(
            @Value("${app.kafka.topics.payment-processed}") String topic,
            KafkaTemplate<String, EventEnvelope<PaymentProcessedData>> kafkaTemplate
    ) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishProcessedPayment(String orderId, EventEnvelope<PaymentProcessedData> event) {
        kafkaTemplate.send(topic, orderId, event);
    }

}
