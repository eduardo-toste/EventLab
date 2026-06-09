package com.project.eventlab.service;

import com.project.eventlab.dto.order.OrderCreatedData;
import com.project.eventlab.dto.order.OrderRequest;
import com.project.eventlab.dto.order.OrderResponse;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.producer.OrderProducer;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderProducer orderProducer;

    public OrderService(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    public OrderResponse createOrder(OrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        OrderCreatedData data = new OrderCreatedData(
                orderId,
                request.customerId(),
                request.total()
        );

        EventEnvelope<OrderCreatedData> event = new EventEnvelope<>(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                "ORDER_CREATED",
                "1",
                LocalDateTime.now(),
                data
        );

        orderProducer.publishOrderCreated(orderId, event);

        return new OrderResponse(
                orderId,
                "CREATED",
                "Order created event published"
        );
    }

}
