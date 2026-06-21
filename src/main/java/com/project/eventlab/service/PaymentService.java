package com.project.eventlab.service;

import com.project.eventlab.dto.order.OrderCreatedData;
import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.enums.PaymentStatus;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.document.PaymentDocument;
import com.project.eventlab.mongo.repository.PaymentRepository;
import com.project.eventlab.producer.PaymentProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class PaymentService {

    private static final BigDecimal MAX_APPROVED_LIMIT = new BigDecimal("1000.00");
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentProducer paymentProducer;
    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentProducer paymentProducer, PaymentRepository paymentRepository) {
        this.paymentProducer = paymentProducer;
        this.paymentRepository = paymentRepository;
    }

    public void processPayment(EventEnvelope<OrderCreatedData> orderCreatedEvent) {
        PaymentDocument paymentDocument = paymentRepository
                .findBySourceEventId(orderCreatedEvent.eventId())
                .orElse(null);

        if (paymentDocument != null) {
            if (paymentDocument.isPublished()) {
                log.info(
                        "[PAYMENT_ALREADY_PUBLISHED] sourceEventId={} orderId={} paymentId={}",
                        paymentDocument.getSourceEventId(),
                        paymentDocument.getOrderId(),
                        paymentDocument.getPaymentId()
                );
                return;
            }

            log.info(
                    "[PAYMENT_REPUBLISHING] sourceEventId={} orderId={} paymentId={}",
                    paymentDocument.getSourceEventId(),
                    paymentDocument.getOrderId(),
                    paymentDocument.getPaymentId()
            );
            publishAndMarkPublished(orderCreatedEvent.correlationId(), paymentDocument);
            return;
        }

        PaymentDocument newPaymentDocument = buildPaymentDocument(orderCreatedEvent);

        try {
            paymentRepository.save(newPaymentDocument);
        } catch (DuplicateKeyException ex) {
            PaymentDocument existingPayment = paymentRepository.findBySourceEventId(orderCreatedEvent.eventId())
                    .orElseThrow(() -> ex);
            publishIfNeeded(orderCreatedEvent.correlationId(), existingPayment);
            return;
        }

        publishAndMarkPublished(orderCreatedEvent.correlationId(), newPaymentDocument);
    }

    private PaymentDocument buildPaymentDocument(EventEnvelope<OrderCreatedData> orderCreatedEvent) {
        OrderCreatedData data = orderCreatedEvent.data();

        PaymentStatus status = PaymentStatus.FAILED;
        String reason = "Amount exceeds approval limit";
        if (data.total() != null && data.total().compareTo(MAX_APPROVED_LIMIT) <= 0) {
            status = PaymentStatus.APPROVED;
            reason = "Payment approved";
        }

        PaymentDocument paymentDocument = new PaymentDocument();
        paymentDocument.setSourceEventId(orderCreatedEvent.eventId());
        paymentDocument.setPaymentId(UUID.randomUUID().toString());
        paymentDocument.setOrderId(data.orderId());
        paymentDocument.setCustomerId(data.customerId());
        paymentDocument.setAmount(data.total());
        paymentDocument.setStatus(status);
        paymentDocument.setReason(reason);
        paymentDocument.setPublished(false);
        paymentDocument.setCreatedAt(LocalDateTime.now());
        return paymentDocument;
    }

    private void publishIfNeeded(String correlationId, PaymentDocument paymentDocument) {
        if (paymentDocument.isPublished()) {
            log.info(
                    "[PAYMENT_ALREADY_PUBLISHED] sourceEventId={} orderId={} paymentId={}",
                    paymentDocument.getSourceEventId(),
                    paymentDocument.getOrderId(),
                    paymentDocument.getPaymentId()
            );
            return;
        }

        log.info(
                "[PAYMENT_REPUBLISHING] sourceEventId={} orderId={} paymentId={}",
                paymentDocument.getSourceEventId(),
                paymentDocument.getOrderId(),
                paymentDocument.getPaymentId()
        );
        publishAndMarkPublished(correlationId, paymentDocument);
    }

    private void publishAndMarkPublished(String correlationId, PaymentDocument paymentDocument) {
        paymentProducer.publishProcessedPayment(
                paymentDocument.getOrderId(),
                buildPaymentProcessedEvent(correlationId, paymentDocument)
        );

        paymentDocument.setPublished(true);
        paymentDocument.setPublishedAt(LocalDateTime.now());
        paymentRepository.save(paymentDocument);
    }

    private EventEnvelope<PaymentProcessedData> buildPaymentProcessedEvent(
            String correlationId,
            PaymentDocument paymentDocument
    ) {
        PaymentProcessedData paymentProcessedData = new PaymentProcessedData(
                paymentDocument.getPaymentId(),
                paymentDocument.getOrderId(),
                paymentDocument.getCustomerId(),
                paymentDocument.getAmount(),
                paymentDocument.getStatus(),
                paymentDocument.getReason()
        );

        return new EventEnvelope<>(
                UUID.randomUUID().toString(),
                correlationId,
                "PAYMENT_PROCESSED",
                "1",
                LocalDateTime.now(),
                paymentProcessedData
        );
    }
}
