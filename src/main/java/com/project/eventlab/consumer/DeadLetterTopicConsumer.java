package com.project.eventlab.consumer;

import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.service.EventLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class DeadLetterTopicConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterTopicConsumer.class);
    private static final String CONSUMER_NAME = "dead-letter-topic-inspector";

    private final EventLogService eventLogService;

    public DeadLetterTopicConsumer(EventLogService eventLogService) {
        this.eventLogService = eventLogService;
    }

    @KafkaListener(
            topics = {
                    "${app.kafka.topics.order-created-dlt}",
                    "${app.kafka.topics.payment-processed-dlt}",
                    "${app.kafka.topics.notification-created-dlt}"
            },
            groupId = CONSUMER_NAME
    )
    public void consume(
            EventEnvelope<?> event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] originalTopic,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) byte[] exceptionMessage
    ) {
        log.error(
                "[DLT_CONSUMED] eventId={} correlationId={} dltTopic={} originalTopic={} errorMessage={}",
                event.eventId(),
                event.correlationId(),
                topic,
                decodeHeader(originalTopic),
                decodeHeader(exceptionMessage)
        );

        eventLogService.saveConsumed(topic, CONSUMER_NAME, event);
    }

    private String decodeHeader(byte[] value) {
        if (value == null) {
            return "unknown";
        }
        return new String(value, StandardCharsets.UTF_8);
    }
}
