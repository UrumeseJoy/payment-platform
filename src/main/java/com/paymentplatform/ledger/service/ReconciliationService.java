package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.EntryType;
import com.paymentplatform.ledger.entity.FindingType;
import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.entity.ReconciliationFinding;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.ledger.repository.ReconciliationFindingRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Detects ledger discrepancies; never fixes them. Read-only with respect to Payment and
 * LedgerEntry — the only writes this class performs are ReconciliationFinding rows.
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final PaymentRepository paymentRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final ReconciliationFindingRepository reconciliationFindingRepository;

    public ReconciliationService(PaymentRepository paymentRepository,
                                  LedgerEntryRepository ledgerEntryRepository,
                                  ReconciliationFindingRepository reconciliationFindingRepository) {
        this.paymentRepository = paymentRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.reconciliationFindingRepository = reconciliationFindingRepository;
    }

    @Scheduled(fixedDelay = 3600000)
    public void runScheduledReconciliation() {
        runReconciliation();
    }

    public ReconciliationSummary runReconciliation() {
        List<PaymentStatus> ledgerAffectingStatuses = Arrays.stream(PaymentStatus.values())
                .filter(LedgerEntry::affectsLedger)
                .toList();

        List<Payment> paymentsToCheck = paymentRepository.findByStatusIn(ledgerAffectingStatuses);

        int findingsCount = 0;
        for (Payment payment : paymentsToCheck) {
            findingsCount += checkPayment(payment);
        }

        findingsCount += checkForOrphanedLedgerEntries(paymentsToCheck);

        log.info("reconciliation complete: {} payments checked, {} findings",
                paymentsToCheck.size(), findingsCount);

        return new ReconciliationSummary(paymentsToCheck.size(), findingsCount);
    }

    private int checkPayment(Payment payment) {
        List<LedgerEntry> entries = ledgerEntryRepository.findByPaymentId(payment.getId());

        if (entries.isEmpty()) {
            recordFinding(payment.getId(), FindingType.MISSING_LEDGER_ENTRIES,
                    "expected ledger entries for payment in status " + payment.getStatus()
                            + ", found 0");
            return 1;
        }

        BigDecimal debitTotal = sumByType(entries, EntryType.DEBIT);
        BigDecimal creditTotal = sumByType(entries, EntryType.CREDIT);

        if (debitTotal.compareTo(creditTotal) != 0) {
            recordFinding(payment.getId(), FindingType.UNBALANCED_LEDGER_ENTRIES,
                    "debit total " + debitTotal + " != credit total " + creditTotal);
            return 1;
        }

        return 0;
    }

    private int checkForOrphanedLedgerEntries(List<Payment> knownAffectingPayments) {
        // ledger_entries.payment_id carries a real database foreign key to payments
        // (REFERENCES payments (id), see V1__init_schema.sql), so this condition cannot occur
        // through any normal write path today — the database itself rejects an insert that
        // would create it. The check is retained anyway as defense-in-depth: a future schema
        // change, a migration mistake, or manual data intervention (e.g. a raw DELETE that
        // bypasses the ORM) could remove or circumvent that constraint without anyone updating
        // this service to match. Because the real schema structurally prevents constructing
        // this scenario against a real Postgres instance, this logic is covered by
        // ReconciliationOrphanDetectionUnitTest (a Mockito unit test with mocked repositories)
        // rather than an integration test.
        //
        // No FK/relationship mapping exists between LedgerEntry.paymentId and Payment.id (it's
        // a raw UUID column, not a @ManyToOne), and this codebase has no precedent anywhere for
        // @Query/native SQL/EntityManager — every existing repository sticks to plain Spring
        // Data derived methods. Rather than introduce a new query mechanism for this one check,
        // this loads all ledger entries and all payment ids (both already small, in-memory-safe
        // collections for this project's scale) and does the orphan check as a Java-side set
        // difference, keeping the whole codebase on one consistent data-access style.
        List<LedgerEntry> allEntries = ledgerEntryRepository.findAll();
        if (allEntries.isEmpty()) {
            return 0;
        }

        Set<UUID> existingPaymentIds = new HashSet<>();
        paymentRepository.findAll().forEach(payment -> existingPaymentIds.add(payment.getId()));

        int findingsCount = 0;
        for (LedgerEntry entry : allEntries) {
            if (!existingPaymentIds.contains(entry.getPaymentId())) {
                recordFinding(entry.getPaymentId(), FindingType.ORPHANED_LEDGER_ENTRY,
                        "ledger entry " + entry.getId() + " (" + entry.getAccount() + "/"
                                + entry.getEntryType() + " " + entry.getAmount()
                                + ") references payment_id " + entry.getPaymentId()
                                + ", which does not exist in payments");
                findingsCount++;
            }
        }
        return findingsCount;
    }

    private BigDecimal sumByType(List<LedgerEntry> entries, EntryType entryType) {
        return entries.stream()
                .filter(entry -> entry.getEntryType() == entryType)
                .map(LedgerEntry::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private void recordFinding(UUID paymentId, FindingType findingType, String detail) {
        log.warn("reconciliation finding: payment {} - {} - {}", paymentId, findingType, detail);
        reconciliationFindingRepository.save(new ReconciliationFinding(paymentId, findingType, detail));
    }
}
