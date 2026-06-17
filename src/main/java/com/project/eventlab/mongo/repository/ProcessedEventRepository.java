package com.project.eventlab.mongo.repository;

import com.project.eventlab.mongo.document.ProcessedEventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;
import java.util.Optional;

public interface ProcessedEventRepository extends MongoRepository<ProcessedEventDocument, String> {

    boolean existsByEventIdAndConsumerName(String eventId, String consumerName);

    Optional<ProcessedEventDocument> findByEventIdAndConsumerName(String eventId, String consumerName);

    List<ProcessedEventDocument> findAllByEventIdAndConsumerNameOrderByCreatedAtDesc(String eventId, String consumerName);

}
