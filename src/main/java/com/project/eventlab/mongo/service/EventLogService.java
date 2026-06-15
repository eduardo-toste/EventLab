package com.project.eventlab.mongo.service;

import com.project.eventlab.event.EventEnvelope;
import com.project.eventlab.mongo.document.EventLogDocument;
import com.project.eventlab.mongo.mapper.MongoMapper;
import com.project.eventlab.mongo.repository.EventLogRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EventLogService {

    private final EventLogRepository eventLogRepository;
    private final MongoMapper mongoMapper;

    public EventLogService(EventLogRepository eventLogRepository, MongoMapper mongoMapper) {
        this.eventLogRepository = eventLogRepository;
        this.mongoMapper = mongoMapper;
    }

    public void savePublished(String topic, String key, EventEnvelope<?> event) {
        eventLogRepository.save(mongoMapper.toPublishedDocument(topic, key,  event));
    }

    public void saveConsumed(String topic, String consumerName, EventEnvelope<?> event) {
        eventLogRepository.save(mongoMapper.toConsumedDocument(topic, consumerName,  event));
    }

    public List<EventLogDocument> findByCorrelationId(String correlationId) {
        return eventLogRepository.findByCorrelationIdOrderByCreatedAtAsc(correlationId);
    }



}
