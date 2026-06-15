package com.project.eventlab.mongo.repository;

import com.project.eventlab.mongo.document.EventLogDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface EventLogRepository extends MongoRepository<EventLogDocument, String> {

    List<EventLogDocument> findByCorrelationIdOrderByCreatedAtAsc(String correlationId);

}
