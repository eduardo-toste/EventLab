package com.project.eventlab.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.eventlab.dto.OrderCreatedData;
import com.project.eventlab.event.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CreatedOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(CreatedOrderConsumer.class);

    private final ObjectMapper objectMapper;

    public CreatedOrderConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "order-created-logger"
    )
    public void consume(EventEnvelope<?> event) {
        OrderCreatedData data = objectMapper.convertValue(event.data(), OrderCreatedData.class);

        log.info(
                "[ORDER_CREATED_CONSUMED] eventId={} correlationId={} orderId={} total={}",
                event.eventId(),
                event.correlationId(),
                data.orderId(),
                data.total()
        );
    }

}
