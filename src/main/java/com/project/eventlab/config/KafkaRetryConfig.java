package com.project.eventlab.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaRetryConfig {

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2L));
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                DuplicateKeyException.class
        );
        return errorHandler;
    }

}
