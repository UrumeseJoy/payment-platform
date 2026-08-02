package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.orchestration.messaging.OutboxEventRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * LedgerService.applyTransition retries up to 3 times on OptimisticLockingFailureException
 * before giving up. LedgerServiceConcurrencyIntegrationTest proves the end-to-end behavior
 * against real Postgres, but real contention timing can't be forced on demand — it can't
 * reliably produce "fails exactly twice then succeeds" or "fails all 3 times" on command.
 * This test pins down that exact retry-count boundary with mocks instead, where the number
 * of failures before success (or exhaustion) is fully under the test's control.
 *
 * applyOnce (the internal @Transactional method) calls paymentRepository.findById on every
 * attempt — it does not reuse a single in-memory Payment across retries — so the mocked
 * findById must return a FRESH Payment instance each time. If it reused a mutated instance,
 * a later attempt would see status already at target and hit the no-op path instead of
 * genuinely re-running transitionTo(). AUTHORIZED is used as the target status throughout
 * because it does not trigger LedgerEntry.affectsLedger, keeping LedgerEntryRepository out
 * of scope entirely (verified below).
 */
class LedgerServiceRetryLogicTest {

    private PaymentRepository paymentRepository;
    private LedgerEntryRepository ledgerEntryRepository;
    private OutboxEventRepository outboxEventRepository;
    private LedgerService ledgerService;
    private UUID paymentId;

    @BeforeEach
    void setUp() {
        paymentRepository = mock(PaymentRepository.class);
        ledgerEntryRepository = mock(LedgerEntryRepository.class);
        outboxEventRepository = mock(OutboxEventRepository.class);
        ledgerService = new LedgerService(paymentRepository, ledgerEntryRepository, outboxEventRepository);
        // No Spring proxy exists in this pure-Mockito test, so self-injection is a plain
        // self-reference here — applyOnce's @Transactional is not actually being tested by
        // this class (it mocks the repositories directly), only the retry-count behavior is.
        ledgerService.setSelf(ledgerService);
        paymentId = UUID.randomUUID();

        // Fresh CREATED-status Payment on every findById call, so each retry attempt
        // legitimately re-runs transitionTo(AUTHORIZED) instead of seeing an already-mutated
        // instance from a previous attempt.
        when(paymentRepository.findById(paymentId)).thenAnswer(invocation ->
                Optional.of(new Payment("merchant-1", new BigDecimal("50.00"), "USD", "key-retry-1")));
    }

    @Test
    void succeedsOnFirstAttempt_noRetryOccurs() {
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        assertThatCode(() -> ledgerService.applyTransition(paymentId, PaymentStatus.AUTHORIZED))
                .doesNotThrowAnyException();

        verify(paymentRepository, times(1)).save(any(Payment.class));
        verify(ledgerEntryRepository, never()).save(any());
    }

    @Test
    void succeedsOnThirdAttempt_retriesTransparentlyThenSucceeds() {
        when(paymentRepository.save(any(Payment.class)))
                .thenThrow(new OptimisticLockingFailureException("attempt 1 lost the race"))
                .thenThrow(new OptimisticLockingFailureException("attempt 2 lost the race"))
                .thenAnswer(invocation -> invocation.getArgument(0));

        assertThatCode(() -> ledgerService.applyTransition(paymentId, PaymentStatus.AUTHORIZED))
                .doesNotThrowAnyException();

        verify(paymentRepository, times(3)).save(any(Payment.class));
        verify(ledgerEntryRepository, never()).save(any());
    }

    @Test
    void exhaustsAllRetries_throwsIllegalStateException() {
        when(paymentRepository.save(any(Payment.class)))
                .thenThrow(new OptimisticLockingFailureException("attempt always loses the race"));

        assertThatThrownBy(() -> ledgerService.applyTransition(paymentId, PaymentStatus.AUTHORIZED))
                .isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(OptimisticLockingFailureException.class);

        // Exactly 3, not 2 or 4: pins down the retry budget's off-by-one boundary.
        verify(paymentRepository, times(3)).save(any(Payment.class));
        verify(ledgerEntryRepository, never()).save(any());
    }
}
