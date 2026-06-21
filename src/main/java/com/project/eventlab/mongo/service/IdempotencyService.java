package com.project.eventlab.mongo.service;

import com.project.eventlab.enums.ProcessingStartDecision;
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

    public ProcessingStartDecision tryStartProcessing(
            String eventId,
            String consumerName,
            String correlationId,
            String eventType
    ) {
        LocalDateTime now = LocalDateTime.now();
        ProcessedEventDocument existingDocument = processedEventRepository
                .findByEventIdAndConsumerName(eventId, consumerName)
                .orElse(null);

        if (existingDocument != null) {
            return resumeExistingDocument(existingDocument, now);
        }

        ProcessedEventDocument document = new ProcessedEventDocument();
        document.setEventId(eventId);
        document.setConsumerName(consumerName);
        document.setCorrelationId(correlationId);
        document.setEventType(eventType);
        document.setStatus(ProcessingStatus.PROCESSING);
        document.setAttemptCount(1);
        document.setCreatedAt(now);
        document.setLastAttemptAt(now);

        try {
            processedEventRepository.save(document);
            return ProcessingStartDecision.STARTED;
        } catch (DuplicateKeyException ex) {
            ProcessedEventDocument savedDocument = findProcessedEvent(eventId, consumerName);
            return resumeExistingDocument(savedDocument, now);
        }
    }

    public void markProcessed(String eventId, String consumerName) {
        ProcessedEventDocument document = findProcessedEvent(eventId, consumerName);

        document.setStatus(ProcessingStatus.PROCESSED);
        document.setProcessedAt(LocalDateTime.now());
        document.setErrorMessage(null);
        processedEventRepository.save(document);
    }

    public void markFailed(String eventId, String consumerName, String errorMessage) {
        ProcessedEventDocument document = findProcessedEvent(eventId, consumerName);

        document.setLastAttemptAt(LocalDateTime.now());
        document.setStatus(ProcessingStatus.FAILED);
        document.setErrorMessage(errorMessage);
        processedEventRepository.save(document);
    }

    private ProcessingStartDecision resumeExistingDocument(ProcessedEventDocument document, LocalDateTime now) {
        if (document.getStatus() == ProcessingStatus.PROCESSED) {
            return ProcessingStartDecision.ALREADY_PROCESSED;
        }

        if (document.getStatus() == ProcessingStatus.PROCESSING) {
            return ProcessingStartDecision.ALREADY_PROCESSING;
        }

        document.setStatus(ProcessingStatus.PROCESSING);
        document.setAttemptCount(document.getAttemptCount() + 1);
        document.setLastAttemptAt(now);
        document.setProcessedAt(null);
        document.setErrorMessage(null);
        processedEventRepository.save(document);

        return ProcessingStartDecision.RETRYING_FAILED;
    }

    private ProcessedEventDocument findProcessedEvent(String eventId, String consumerName) {
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
