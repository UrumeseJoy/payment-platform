package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.EntryType;
import com.paymentplatform.ledger.entity.FindingType;
import com.paymentplatform.ledger.entity.LedgerAccount;
import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.entity.ReconciliationFinding;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.ledger.repository.ReconciliationFindingRepository;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ReconciliationServiceIntegrationTest covers the other three finding types against a real
 * Postgres database, but ORPHANED_LEDGER_ENTRY cannot be covered there: ledger_entries.payment_id
 * has a real foreign key to payments (V1__init_schema.sql), so a real database will always
 * reject the exact row this scenario needs before ReconciliationService ever gets to see it —
 * confirmed by actually attempting it and getting a DataIntegrityViolationException, not assumed.
 * This test exists only to prove the Java-side set-difference detection LOGIC in
 * ReconciliationService is correct in isolation, using mocked repositories that don't enforce
 * that constraint. It intentionally does not claim this scenario occurs in production — with the
 * schema as it stands today, it cannot.
 */
class ReconciliationOrphanDetectionUnitTest {

    @Test
    void orphanedLedgerEntry_noMatchingPayment_detected() {
        PaymentRepository paymentRepository = mock(PaymentRepository.class);
        LedgerEntryRepository ledgerEntryRepository = mock(LedgerEntryRepository.class);
        ReconciliationFindingRepository reconciliationFindingRepository = mock(ReconciliationFindingRepository.class);

        ReconciliationService reconciliationService = new ReconciliationService(
                paymentRepository, ledgerEntryRepository, reconciliationFindingRepository);

        when(paymentRepository.findByStatusIn(any())).thenReturn(List.of());
        when(paymentRepository.findAll()).thenReturn(List.of());

        UUID orphanPaymentId = UUID.randomUUID();
        LedgerEntry orphanEntry = new LedgerEntry(orphanPaymentId, LedgerAccount.PLATFORM_SUSPENSE,
                EntryType.DEBIT, new BigDecimal("25.00"), PaymentStatus.CAPTURED);
        when(ledgerEntryRepository.findAll()).thenReturn(List.of(orphanEntry));

        reconciliationService.runReconciliation();

        ArgumentCaptor<ReconciliationFinding> findingCaptor =
                ArgumentCaptor.forClass(ReconciliationFinding.class);
        verify(reconciliationFindingRepository).save(findingCaptor.capture());

        ReconciliationFinding finding = findingCaptor.getValue();
        assertThat(finding.getPaymentId()).isEqualTo(orphanPaymentId);
        assertThat(finding.getFindingType()).isEqualTo(FindingType.ORPHANED_LEDGER_ENTRY);
    }
}
