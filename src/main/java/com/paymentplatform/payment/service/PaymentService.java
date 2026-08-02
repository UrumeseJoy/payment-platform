package com.paymentplatform.payment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.payment.dto.CreatePaymentRequest;
import com.paymentplatform.payment.dto.PaymentResponse;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentNotReversibleException;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import com.paymentplatform.orchestration.messaging.OutboxEvent;
import com.paymentplatform.orchestration.messaging.OutboxEventRepository;
import com.paymentplatform.orchestration.messaging.PaymentEvent;
import com.paymentplatform.orchestration.messaging.PaymentEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    // Self-injected proxy, used only to invoke createPaymentAndOutboxEvent through Spring's
    // transactional advice. Calling that method via `this.` (self-invocation) would silently
    // skip @Transactional entirely, since AOP proxies only intercept calls that arrive through
    // the proxy — see the class-level note on createPaymentAndOutboxEvent for why that method
    // specifically cannot be folded into createPayment's own body.
    private final PaymentService self;

    @Autowired
    public PaymentService(PaymentRepository paymentRepository,
                           OutboxEventRepository outboxEventRepository,
                           RedisTemplate<String, String> redisTemplate,
                           ObjectMapper objectMapper,
                           @Lazy PaymentService self) {
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.self = self;
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
            // Dispatched through the proxy (self.___, not this.___) so the payment insert and
            // the outbox insert genuinely share one transaction. If this were an in-class call,
            // @Transactional below would be silently ignored and the two writes could commit
            // independently — reintroducing the exact crash window the outbox pattern exists
            // to close.
            payment = self.createPaymentAndOutboxEvent(payment);
        } catch (DataIntegrityViolationException e) {
            // Deliberately handled OUTSIDE any transaction of createPayment's own: if this
            // catch ran inside a transaction shared with the failed save(), the save()'s own
            // transactional advice would have already marked that transaction rollback-only,
            // and returning normally afterward would throw UnexpectedRollbackException instead
            // of the recovered response below.
            Payment winner = paymentRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Constraint violation but no payment found for key: " + idempotencyKey));
            return toResponse(winner);
        }

        PaymentResponse response = toResponse(payment);
        cache(redisKey, response);
        return response;
    }

    @Transactional
    protected Payment createPaymentAndOutboxEvent(Payment payment) {
        Payment saved = paymentRepository.save(payment);

        PaymentEvent event = new PaymentEvent(saved.getId(), PaymentEventType.PAYMENT_CREATED, saved.getAmount());
        outboxEventRepository.save(new OutboxEvent(saved.getId(), PaymentEventType.PAYMENT_CREATED.name(),
                serialize(event)));

        return saved;
    }

    @Transactional
    public void reversePayment(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new IllegalArgumentException("No payment found: " + paymentId));

        if (payment.getStatus() != PaymentStatus.CAPTURED) {
            throw new PaymentNotReversibleException(payment.getStatus());
        }

        PaymentEvent event = new PaymentEvent(payment.getId(), PaymentEventType.PAYMENT_REVERSED, payment.getAmount());
        outboxEventRepository.save(new OutboxEvent(payment.getId(), PaymentEventType.PAYMENT_REVERSED.name(),
                serialize(event)));
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

    private String serialize(PaymentEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize payment event for payment " + event.paymentId(), e);
        }
    }
}
