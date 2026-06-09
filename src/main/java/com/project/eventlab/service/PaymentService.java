package com.project.eventlab.service;

import com.project.eventlab.dto.order.OrderCreatedData;
import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.enums.PaymentStatus;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.producer.PaymentProducer;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class PaymentService {

    private static final BigDecimal MAX_APPROVED_LIMIT = new BigDecimal("1000.00");

    private final PaymentProducer paymentProducer;

    public PaymentService(PaymentProducer paymentProducer) {
        this.paymentProducer = paymentProducer;
    }

    public void processPayment(EventEnvelope<OrderCreatedData> orderCreatedEvent) {
        OrderCreatedData data = orderCreatedEvent.data();

        PaymentStatus status = PaymentStatus.FAILED;
        String reason = "Amount exceeds approval limit";
        if (data.total() != null && data.total().compareTo(MAX_APPROVED_LIMIT) <= 0) {
            status = PaymentStatus.APPROVED;
            reason = "Payment approved";
        }

        PaymentProcessedData paymentProcessedData = new PaymentProcessedData(
                UUID.randomUUID().toString(),
                data.orderId(),
                data.customerId(),
                data.total(),
                status,
                reason
        );

        EventEnvelope<PaymentProcessedData> paymentProcessedEvent = new EventEnvelope<>(
                UUID.randomUUID().toString(),
                orderCreatedEvent.correlationId(),
                "PAYMENT_PROCESSED",
                "1",
                LocalDateTime.now(),
                paymentProcessedData
        );

        paymentProducer.publishProcessedPayment(data.orderId(), paymentProcessedEvent);
    }

}
