package com.project.eventlab.mongo.repository;

import com.project.eventlab.mongo.document.ProcessedEventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface ProcessedEventRepository extends MongoRepository<ProcessedEventDocument, String> {

    Optional<ProcessedEventDocument> findByEventIdAndConsumerName(String eventId, String consumerName);

}
