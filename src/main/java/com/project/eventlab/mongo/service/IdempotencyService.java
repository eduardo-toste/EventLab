package com.project.eventlab.mongo.service;

import com.mongodb.DuplicateKeyException;
import com.project.eventlab.enums.ProcessingStatus;
import com.project.eventlab.mongo.document.ProcessedEventDocument;
import com.project.eventlab.mongo.repository.ProcessedEventRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class IdempotencyService {

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
        ProcessedEventDocument document = processedEventRepository
                .findByEventIdAndConsumerName(eventId, consumerName)
                .orElseThrow(() -> new IllegalArgumentException("Processed event not found"));

        document.setStatus(ProcessingStatus.PROCESSED);
        document.setProcessedAt(LocalDateTime.now());
        processedEventRepository.save(document);
    }

    public void markFailed(String eventId, String consumerName, String errorMessage) {
        ProcessedEventDocument document = processedEventRepository
                .findByEventIdAndConsumerName(eventId, consumerName)
                .orElseThrow(() -> new IllegalStateException("Processed event not found"));

        document.setStatus(ProcessingStatus.FAILED);
        document.setErrorMessage(errorMessage);
        processedEventRepository.save(document);
    }

}
