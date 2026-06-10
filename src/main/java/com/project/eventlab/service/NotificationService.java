package com.project.eventlab.service;

import com.project.eventlab.dto.notification.NotificationCreatedData;
import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.enums.NotificationChannel;
import com.project.eventlab.enums.PaymentStatus;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.producer.NotificationProducer;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class NotificationService {

    private final NotificationProducer notificationProducer;

    public NotificationService(NotificationProducer notificationProducer) {
        this.notificationProducer = notificationProducer;
    }

    public void sendNotification(EventEnvelope<PaymentProcessedData> paymentProcessedEvent) {
        PaymentProcessedData data = paymentProcessedEvent.data();
        String notificationMessage = paymentProcessedEvent.data().status().equals(PaymentStatus.APPROVED) ? "Your payment was approved" : "Your payment was not approved";

        NotificationCreatedData notificationCreatedData = new NotificationCreatedData(
                UUID.randomUUID().toString(),
                data.orderId(),
                data.customerId(),
                NotificationChannel.EMAIL,
                notificationMessage,
                "CREATED"
        );

        EventEnvelope<NotificationCreatedData> notificationCreatedEvent = new EventEnvelope<>(
                UUID.randomUUID().toString(),
                paymentProcessedEvent.correlationId(),
                "NOTIFICATION_CREATED",
                "1",
                paymentProcessedEvent.occurredAt(),
                notificationCreatedData
        );

        notificationProducer.publishNotification(data.orderId(), notificationCreatedEvent);
    }

}
