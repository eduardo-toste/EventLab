package com.project.eventlab.consumer;

import com.project.eventlab.dto.OrderCreatedData;
import com.project.eventlab.event.EventEnvelope;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CreatedOrderConsumer {

    @KafkaListener(
            topics = "order.created",
            groupId = "order-created-logger"
    )
    public void consume(EventEnvelope<OrderCreatedData> event) {
        System.out.println(event);
    }

}
