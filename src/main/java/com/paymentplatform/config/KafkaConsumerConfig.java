package com.paymentplatform.config;

import com.paymentplatform.payment.entity.IllegalStateTransitionException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(backOff);
        errorHandler.addNotRetryableExceptions(IllegalStateTransitionException.class, IllegalArgumentException.class);
        return errorHandler;
    }
}
