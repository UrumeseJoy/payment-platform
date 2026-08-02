package com.paymentplatform.orchestration.service;

import com.paymentplatform.orchestration.messaging.OutboxEvent;
import com.paymentplatform.orchestration.messaging.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Polls outbox_events for unpublished rows and relays them onto Kafka, on a fixed delay.
 *
 * This is an AT-LEAST-ONCE relay, not exactly-once: if the process crashes after a Kafka send
 * succeeds but before published_at is persisted, that same row is still NULL on the next cycle
 * and will be sent again. That means every downstream consumer of these events — in practice
 * just PaymentEventConsumer today — must already tolerate duplicate delivery of the same event.
 *
 * Checked against the current PaymentEventConsumer logic: the ledger/status write itself IS
 * safe to repeat, because LedgerService.applyTransition no-ops when the payment is already at
 * the target status. However, PaymentEventConsumer's handling of PAYMENT_CREATED is NOT fully
 * duplicate-safe beyond that: handlePaymentCreated calls paymentGateway.authorize(...)
 * unconditionally on every delivery, regardless of the payment's current status, and on a
 * SUCCESS outcome it unconditionally re-publishes a PAYMENT_AUTHORIZED event even when the
 * ledger write underneath it was a no-op. So a redelivered PAYMENT_CREATED (e.g. after this
 * relay resends a row it already sent once) would trigger a second real gateway authorization
 * call and a second downstream PAYMENT_AUTHORIZED publish, even though it can no longer corrupt
 * the ledger/payment state itself. With today's MockPaymentGateway this has no visible effect
 * since it's stateless and deterministic, but a real gateway integration would need its own
 * idempotency key (e.g. keyed by payment id) before this relay's at-least-once guarantee is
 * actually safe end to end. This is flagged here rather than silently assumed safe.
 */
@Service
public class OutboxRelayService {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);
    private static final String TOPIC = "payment-events";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelayService(OutboxEventRepository outboxEventRepository,
                               KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 5000)
    public void relayPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();

        for (OutboxEvent outboxEvent : pending) {
            try {
                kafkaTemplate.send(TOPIC, outboxEvent.getAggregateId().toString(), outboxEvent.getPayload());
                outboxEvent.markPublished(Instant.now());
                outboxEventRepository.save(outboxEvent);
            } catch (Exception e) {
                log.error("Failed to relay outbox event {} (eventType={}); will retry next cycle",
                        outboxEvent.getId(), outboxEvent.getEventType(), e);
            }
        }
    }
}
