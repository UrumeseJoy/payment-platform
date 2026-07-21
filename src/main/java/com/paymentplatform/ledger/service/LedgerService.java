package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
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

    public LedgerService(PaymentRepository paymentRepository, LedgerEntryRepository ledgerEntryRepository) {
        this.paymentRepository = paymentRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
    }

    public void applyTransition(UUID paymentId, PaymentStatus targetStatus) {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                applyOnce(paymentId, targetStatus);
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
    protected void applyOnce(UUID paymentId, PaymentStatus targetStatus) {
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
    }
}
