package com.project.eventlab.producer;

import com.project.eventlab.dto.order.OrderCreatedData;
import com.project.eventlab.event.EventEnvelope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer {

    private final String topic;
    private final KafkaTemplate<String, EventEnvelope<OrderCreatedData>> kafkaTemplate;

    public OrderProducer(
            KafkaTemplate<String, EventEnvelope<OrderCreatedData>> kafkaTemplate,
            @Value("${app.kafka.topics.order-created}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishOrderCreated(String orderId, EventEnvelope<OrderCreatedData> event) {
        kafkaTemplate.send(topic, orderId, event);
    }

}
