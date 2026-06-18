package com.project.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaRetryConfig {

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        FixedBackOff backOff = new FixedBackOff(1000L, 2);
        return new DefaultErrorHandler(backOff);
    }

}
