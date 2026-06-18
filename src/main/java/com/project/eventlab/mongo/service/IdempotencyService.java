package com.project.eventlab.mongo.service;

import com.project.eventlab.enums.ProcessingStatus;
import com.project.eventlab.mongo.document.ProcessedEventDocument;
import com.project.eventlab.mongo.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);

    private final ProcessedEventRepository processedEventRepository;

    public IdempotencyService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    public boolean tryStartProcessing(
            String eventId,
            String consumerName,
            String correlationId,
            String eventType
    ) {
        if (processedEventRepository.existsByEventIdAndConsumerName(eventId, consumerName)) {
            return false;
        }

        ProcessedEventDocument document = new ProcessedEventDocument();
        document.setEventId(eventId);
        document.setConsumerName(consumerName);
        document.setCorrelationId(correlationId);
        document.setEventType(eventType);
        document.setStatus(ProcessingStatus.PROCESSING);
        document.setCreatedAt(LocalDateTime.now());

        try {
            processedEventRepository.save(document);
            return true;
        } catch (DuplicateKeyException ex) {
            return false;
        }
    }

    public void markProcessed(String eventId, String consumerName) {
        ProcessedEventDocument document = findLatestProcessedEvent(eventId, consumerName);

        document.setStatus(ProcessingStatus.PROCESSED);
        document.setProcessedAt(LocalDateTime.now());
        processedEventRepository.save(document);
    }

    public void markFailed(String eventId, String consumerName, String errorMessage) {
        ProcessedEventDocument document = findLatestProcessedEvent(eventId, consumerName);

        document.setStatus(ProcessingStatus.FAILED);
        document.setErrorMessage(errorMessage);
        processedEventRepository.save(document);
    }

    private ProcessedEventDocument findLatestProcessedEvent(String eventId, String consumerName) {
        List<ProcessedEventDocument> processedEvents = processedEventRepository
                .findAllByEventIdAndConsumerNameOrderByCreatedAtDesc(eventId, consumerName);

        if (processedEvents.isEmpty()) {
            throw new IllegalStateException("Processed event not found");
        }

        if (processedEvents.size() > 1) {
            log.warn(
                    "[IDEMPOTENCY_DUPLICATE_RECORDS] eventId={} consumerName={} duplicates={}",
                    eventId,
                    consumerName,
                    processedEvents.size()
            );
        }

        return processedEvents.getFirst();
    }

}
