package com.project.eventlab.service;

import com.project.eventlab.dto.notification.NotificationCreatedData;
import com.project.eventlab.dto.payment.PaymentProcessedData;
import com.project.eventlab.enums.NotificationChannel;
import com.project.eventlab.enums.PaymentStatus;
import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.document.NotificationDocument;
import com.project.eventlab.mongo.repository.NotificationRepository;
import com.project.eventlab.producer.NotificationProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationProducer notificationProducer;
    private final NotificationRepository notificationRepository;

    public NotificationService(NotificationProducer notificationProducer, NotificationRepository notificationRepository) {
        this.notificationProducer = notificationProducer;
        this.notificationRepository = notificationRepository;
    }

    public void sendNotification(EventEnvelope<PaymentProcessedData> paymentProcessedEvent) {
        NotificationDocument notificationDocument = notificationRepository
                .findBySourceEventId(paymentProcessedEvent.eventId())
                .orElse(null);

        if (notificationDocument != null) {
            if (notificationDocument.isPublished()) {
                log.info(
                        "[NOTIFICATION_ALREADY_PUBLISHED] sourceEventId={} orderId={} notificationId={}",
                        notificationDocument.getSourceEventId(),
                        notificationDocument.getOrderId(),
                        notificationDocument.getNotificationId()
                );
                return;
            }

            log.info(
                    "[NOTIFICATION_REPUBLISHING] sourceEventId={} orderId={} notificationId={}",
                    notificationDocument.getSourceEventId(),
                    notificationDocument.getOrderId(),
                    notificationDocument.getNotificationId()
            );
            publishAndMarkPublished(paymentProcessedEvent.correlationId(), notificationDocument);
            return;
        }

        NotificationDocument newNotificationDocument = buildNotificationDocument(paymentProcessedEvent);

        try {
            notificationRepository.save(newNotificationDocument);
        } catch (DuplicateKeyException ex) {
            NotificationDocument existingNotification = notificationRepository
                    .findBySourceEventId(paymentProcessedEvent.eventId())
                    .orElseThrow(() -> ex);
            publishIfNeeded(paymentProcessedEvent.correlationId(), existingNotification);
            return;
        }

        publishAndMarkPublished(paymentProcessedEvent.correlationId(), newNotificationDocument);
    }

    private NotificationDocument buildNotificationDocument(EventEnvelope<PaymentProcessedData> paymentProcessedEvent) {
        PaymentProcessedData data = paymentProcessedEvent.data();
        String notificationMessage = data.status() == PaymentStatus.APPROVED
                ? "Your payment was approved"
                : "Your payment was not approved";

        NotificationDocument notificationDocument = new NotificationDocument();
        notificationDocument.setSourceEventId(paymentProcessedEvent.eventId());
        notificationDocument.setNotificationId(UUID.randomUUID().toString());
        notificationDocument.setOrderId(data.orderId());
        notificationDocument.setCustomerId(data.customerId());
        notificationDocument.setChannel(NotificationChannel.EMAIL);
        notificationDocument.setMessage(notificationMessage);
        notificationDocument.setStatus("CREATED");
        notificationDocument.setPublished(false);
        notificationDocument.setCreatedAt(LocalDateTime.now());
        return notificationDocument;
    }

    private void publishIfNeeded(String correlationId, NotificationDocument notificationDocument) {
        if (notificationDocument.isPublished()) {
            log.info(
                    "[NOTIFICATION_ALREADY_PUBLISHED] sourceEventId={} orderId={} notificationId={}",
                    notificationDocument.getSourceEventId(),
                    notificationDocument.getOrderId(),
                    notificationDocument.getNotificationId()
            );
            return;
        }

        log.info(
                "[NOTIFICATION_REPUBLISHING] sourceEventId={} orderId={} notificationId={}",
                notificationDocument.getSourceEventId(),
                notificationDocument.getOrderId(),
                notificationDocument.getNotificationId()
        );
        publishAndMarkPublished(correlationId, notificationDocument);
    }

    private void publishAndMarkPublished(String correlationId, NotificationDocument notificationDocument) {
        notificationProducer.publishNotification(
                notificationDocument.getOrderId(),
                buildNotificationCreatedEvent(correlationId, notificationDocument)
        );

        notificationDocument.setPublished(true);
        notificationDocument.setPublishedAt(LocalDateTime.now());
        notificationRepository.save(notificationDocument);
    }

    private EventEnvelope<NotificationCreatedData> buildNotificationCreatedEvent(
            String correlationId,
            NotificationDocument notificationDocument
    ) {
        NotificationCreatedData notificationCreatedData = new NotificationCreatedData(
                notificationDocument.getNotificationId(),
                notificationDocument.getOrderId(),
                notificationDocument.getCustomerId(),
                notificationDocument.getChannel(),
                notificationDocument.getMessage(),
                notificationDocument.getStatus()
        );

        return new EventEnvelope<>(
                UUID.randomUUID().toString(),
                correlationId,
                "NOTIFICATION_CREATED",
                "1",
                LocalDateTime.now(),
                notificationCreatedData
        );
    }

}
