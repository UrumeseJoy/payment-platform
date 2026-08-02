package com.paymentplatform.orchestration.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.gateway.GatewayResult;
import com.paymentplatform.gateway.PaymentGateway;
import com.paymentplatform.ledger.service.LedgerService;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final PaymentGateway paymentGateway;
    private final LedgerService ledgerService;
    private final PaymentRepository paymentRepository;
    private final ObjectMapper objectMapper;

    public PaymentEventConsumer(PaymentGateway paymentGateway,
                                 LedgerService ledgerService,
                                 PaymentRepository paymentRepository,
                                 ObjectMapper objectMapper) {
        this.paymentGateway = paymentGateway;
        this.ledgerService = ledgerService;
        this.paymentRepository = paymentRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "payment-events", groupId = "payment-orchestration")
    public void onMessage(String message) {
        PaymentEvent event = deserialize(message);
        log.info("Received event {} for payment {}", event.eventType(), event.paymentId());

        switch (event.eventType()) {
            case PAYMENT_CREATED -> handlePaymentCreated(event);
            case PAYMENT_AUTHORIZED -> handlePaymentAuthorized(event);
            case PAYMENT_REVERSED -> ledgerService.applyTransition(event.paymentId(), PaymentStatus.REVERSED);
            default -> throw new IllegalArgumentException("Unsupported event type: " + event.eventType());
        }
    }

    private void handlePaymentCreated(PaymentEvent event) {
        Payment payment = paymentRepository.findById(event.paymentId())
                .orElseThrow(() -> new IllegalArgumentException("Payment not found: " + event.paymentId()));

        if (payment.getStatus() != PaymentStatus.CREATED) {
            log.info("skipping duplicate delivery for payment {}, already at status {}",
                    event.paymentId(), payment.getStatus());
            return;
        }

        GatewayResult result = paymentGateway.authorize(event.paymentId(), event.amount());

        switch (result.outcome()) {
            case SUCCESS -> {
                PaymentEvent authorizedEvent =
                        new PaymentEvent(event.paymentId(), PaymentEventType.PAYMENT_AUTHORIZED, event.amount());
                ledgerService.applyTransition(event.paymentId(), PaymentStatus.AUTHORIZED,
                        PaymentEventType.PAYMENT_AUTHORIZED.name(), serialize(authorizedEvent));
            }
            case DECLINED -> {
                ledgerService.applyTransition(event.paymentId(), PaymentStatus.FAILED);
                log.warn("Authorization declined for payment {}: {}", event.paymentId(), result.message());
            }
            case TIMEOUT -> throw new GatewayTimeoutException(
                    "Gateway timed out authorizing payment " + event.paymentId());
        }
    }

    private void handlePaymentAuthorized(PaymentEvent event) {
        Payment payment = paymentRepository.findById(event.paymentId())
                .orElseThrow(() -> new IllegalArgumentException("Payment not found: " + event.paymentId()));

        if (payment.getStatus() != PaymentStatus.AUTHORIZED) {
            log.info("skipping duplicate delivery for payment {}, already at status {}",
                    event.paymentId(), payment.getStatus());
            return;
        }

        GatewayResult result = paymentGateway.capture(event.paymentId(), event.amount());

        switch (result.outcome()) {
            case SUCCESS -> {
                // Settlement is a separate, typically batched process (often T+1/T+2
                // after capture) run independently of the capture flow — it is not an
                // instantaneous side effect of a successful capture. v1 does not
                // implement a settlement batch job, so CAPTURED is the practical
                // terminal success state for now; SETTLED remains a valid transition
                // in the state machine, just not one triggered from here.
                ledgerService.applyTransition(event.paymentId(), PaymentStatus.CAPTURED);
            }
            case DECLINED -> {
                ledgerService.applyTransition(event.paymentId(), PaymentStatus.FAILED);
                log.warn("Capture declined for payment {}: {}", event.paymentId(), result.message());
            }
            case TIMEOUT -> throw new GatewayTimeoutException(
                    "Gateway timed out capturing payment " + event.paymentId());
        }
    }

    private PaymentEvent deserialize(String message) {
        try {
            return objectMapper.readValue(message, PaymentEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to deserialize payment event: " + message, e);
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
