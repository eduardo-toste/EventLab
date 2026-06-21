package com.project.eventlab.mongo.repository;

import com.project.eventlab.mongo.document.PaymentDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface PaymentRepository extends MongoRepository<PaymentDocument, String> {

    Optional<PaymentDocument> findBySourceEventId(String sourceEventId);

}
