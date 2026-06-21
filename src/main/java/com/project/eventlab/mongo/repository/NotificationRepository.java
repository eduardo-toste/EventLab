package com.project.eventlab.mongo.repository;

import com.project.eventlab.mongo.document.NotificationDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface NotificationRepository extends MongoRepository<NotificationDocument, String> {

    Optional<NotificationDocument> findBySourceEventId(String sourceEventId);

}
