package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.orchestration.messaging.OutboxEvent;
import com.paymentplatform.orchestration.messaging.OutboxEventRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class LedgerService {

    private static final int MAX_ATTEMPTS = 3;

    private final PaymentRepository paymentRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final OutboxEventRepository outboxEventRepository;

    // Self-injected proxy, used only to invoke applyOnce through Spring's transactional
    // advice. applyTransition previously called applyOnce directly (this.applyOnce(...)),
    // which is a self-invocation that Spring's CGLIB proxy never sees — @Transactional on
    // applyOnce was silently a no-op as a result, so the read/transition/save/ledger-write
    // sequence was never actually one atomic unit. Wired via setter injection rather than
    // constructor injection so plain-Mockito unit tests (LedgerServiceRetryLogicTest) can
    // still construct this class directly without needing a Spring proxy of itself.
    private LedgerService self;

    public LedgerService(PaymentRepository paymentRepository,
                          LedgerEntryRepository ledgerEntryRepository,
                          OutboxEventRepository outboxEventRepository) {
        this.paymentRepository = paymentRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @Autowired
    public void setSelf(@Lazy LedgerService self) {
        this.self = self;
    }

    public void applyTransition(UUID paymentId, PaymentStatus targetStatus) {
        applyTransition(paymentId, targetStatus, null, null);
    }

    /**
     * Same transition as the two-arg overload, but when the transition is actually applied
     * (not a no-op), also persists an outbox_events row in the SAME transactional attempt as
     * the status change — so a chained downstream event can never be recorded without the
     * transition it depends on having actually committed, or vice versa. If the transition is
     * a no-op (payment already at targetStatus), no outbox row is written, since nothing new
     * happened that a downstream consumer needs to hear about.
     */
    public void applyTransition(UUID paymentId, PaymentStatus targetStatus,
                                 String outboxEventType, String outboxPayload) {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                self.applyOnce(paymentId, targetStatus, outboxEventType, outboxPayload);
                return;
            } catch (OptimisticLockingFailureException e) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw new IllegalStateException(
                            "Failed to apply transition for payment " + paymentId + " to " + targetStatus
                                    + " after " + attempt + " attempts", e);
                }
            }
        }
    }

    @Transactional
    protected void applyOnce(UUID paymentId, PaymentStatus targetStatus,
                              String outboxEventType, String outboxPayload) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new IllegalArgumentException("Payment not found: " + paymentId));

        if (payment.getStatus() == targetStatus) {
            return;
        }

        payment.transitionTo(targetStatus);
        paymentRepository.save(payment);

        if (LedgerEntry.affectsLedger(targetStatus)) {
            List<LedgerEntry> entries = LedgerEntry.pairFor(paymentId, targetStatus, payment.getAmount());
            ledgerEntryRepository.saveAll(entries);
        }

        if (outboxEventType != null) {
            outboxEventRepository.save(new OutboxEvent(paymentId, outboxEventType, outboxPayload));
        }
    }
}
