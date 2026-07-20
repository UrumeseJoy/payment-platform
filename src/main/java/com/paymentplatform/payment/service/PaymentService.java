package com.paymentplatform.payment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.payment.dto.CreatePaymentRequest;
import com.paymentplatform.payment.dto.PaymentResponse;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public PaymentService(PaymentRepository paymentRepository,
                           RedisTemplate<String, String> redisTemplate,
                           ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    public PaymentResponse createPayment(CreatePaymentRequest request, String idempotencyKey) {
        String redisKey = "idempotency:" + idempotencyKey;

        String cached = redisTemplate.opsForValue().get(redisKey);
        if (cached != null) {
            return deserialize(cached);
        }

        Optional<Payment> existing = paymentRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            PaymentResponse response = toResponse(existing.get());
            cache(redisKey, response);
            return response;
        }

        Payment payment = new Payment(request.getMerchantId(), request.getAmount(),
                request.getCurrency(), idempotencyKey);

        try {
            payment = paymentRepository.save(payment);
        } catch (DataIntegrityViolationException e) {
            Payment winner = paymentRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Constraint violation but no payment found for key: " + idempotencyKey));
            return toResponse(winner);
        }

        PaymentResponse response = toResponse(payment);
        cache(redisKey, response);
        return response;
    }

    private PaymentResponse toResponse(Payment payment) {
        return new PaymentResponse(
                payment.getId(),
                payment.getMerchantId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus().name(),
                payment.getCreatedAt()
        );
    }

    private void cache(String redisKey, PaymentResponse response) {
        try {
            String json = objectMapper.writeValueAsString(response);
            redisTemplate.opsForValue().set(redisKey, json, Duration.ofHours(24));
        } catch (JsonProcessingException e) {
            log.warn("Failed to cache idempotency response for key {}", redisKey, e);
        }
    }

    private PaymentResponse deserialize(String json) {
        try {
            return objectMapper.readValue(json, PaymentResponse.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupted idempotency cache entry", e);
        }
    }
}