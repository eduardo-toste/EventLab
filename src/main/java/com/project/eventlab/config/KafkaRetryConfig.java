package com.project.eventlab.config;

import com.project.eventlab.event.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaRetryConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaRetryConfig.class);

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaOperations<Object, Object> kafkaOperations,
            @Value("${app.kafka.topics.order-created-dlt}") String orderCreatedDltTopic,
            @Value("${app.kafka.topics.payment-processed-dlt}") String paymentProcessedDltTopic,
            @Value("${app.kafka.topics.notification-created-dlt}") String notificationCreatedDltTopic
    ) {
        return new DeadLetterPublishingRecoverer(
                kafkaOperations,
                (record, exception) -> resolveDltTopic(
                        record,
                        exception,
                        orderCreatedDltTopic,
                        paymentProcessedDltTopic,
                        notificationCreatedDltTopic
                )
        );
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                DuplicateKeyException.class
        );
        return errorHandler;
    }

    private TopicPartition resolveDltTopic(
            ConsumerRecord<?, ?> record,
            Exception exception,
            String orderCreatedDltTopic,
            String paymentProcessedDltTopic,
            String notificationCreatedDltTopic
    ) {
        String dltTopic = switch (record.topic()) {
            case "order.created" -> orderCreatedDltTopic;
            case "payment.processed" -> paymentProcessedDltTopic;
            case "notification.created" -> notificationCreatedDltTopic;
            default -> record.topic() + ".dlt";
        };

        String eventId = extractEventId(record.value());
        String correlationId = extractCorrelationId(record.value());

        log.error(
                "[DLT_PUBLISHING] sourceTopic={} dltTopic={} partition={} offset={} eventId={} correlationId={} exception={}",
                record.topic(),
                dltTopic,
                record.partition(),
                record.offset(),
                eventId,
                correlationId,
                exception.getClass().getSimpleName()
        );

        return new TopicPartition(dltTopic, record.partition());
    }

    private String extractEventId(Object value) {
        if (value instanceof EventEnvelope<?> eventEnvelope) {
            return eventEnvelope.eventId();
        }
        return "unknown";
    }

    private String extractCorrelationId(Object value) {
        if (value instanceof EventEnvelope<?> eventEnvelope) {
            return eventEnvelope.correlationId();
        }
        return "unknown";
    }

}
