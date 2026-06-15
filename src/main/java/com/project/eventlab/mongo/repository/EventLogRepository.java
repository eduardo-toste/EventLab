package com.project.eventlab.mongo.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface EventLogRepository extends MongoRepository<EventLogRepository, String> {
}
