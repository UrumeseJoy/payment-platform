package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.EntryType;
import com.paymentplatform.ledger.entity.FindingType;
import com.paymentplatform.ledger.entity.LedgerAccount;
import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.entity.ReconciliationFinding;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.ledger.repository.ReconciliationFindingRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers ReconciliationService's detection logic in isolation, against directly-inserted
 * repository data — no Kafka, no LedgerService, no gateway. This class proves the DETECTION
 * rules themselves are correct (each finding type fires when it should, and the healthy case
 * produces zero false positives) using data shaped by hand. It deliberately does NOT prove that
 * the real production write path (Kafka -> PaymentEventConsumer -> LedgerService) produces data
 * reconciliation agrees with — that's ReconciliationServiceCleanStateTest's job, in its own
 * file, because it needs a full app context with real Kafka rather than this narrow slice.
 *
 * ORPHANED_LEDGER_ENTRY is deliberately NOT covered here: ledger_entries.payment_id carries a
 * real database foreign key to payments (REFERENCES payments (id), see V1__init_schema.sql) —
 * invisible from the JPA entity mapping since there's no @ManyToOne, but very real at the schema
 * level. Attempting to insert a LedgerEntry row for a payment_id that was never saved throws a
 * DataIntegrityViolationException here, so this scenario cannot be created against the real
 * database at all. See ReconciliationOrphanDetectionUnitTest for that check's coverage instead —
 * a pure-Mockito test proving the Java-side detection LOGIC is correct, explicitly documented as
 * covering something unreachable in this schema as it stands today.
 */
@Testcontainers
@SpringBootTest(
        classes = ReconciliationServiceIntegrationTest.TestConfig.class,
        properties = "spring.data.redis.repositories.enabled=false")
class ReconciliationServiceIntegrationTest {

    @Configuration
    @EnableAutoConfiguration
    @EntityScan(basePackageClasses = {Payment.class, LedgerEntry.class, ReconciliationFinding.class})
    @EnableJpaRepositories(basePackageClasses = {PaymentRepository.class, LedgerEntryRepository.class,
            ReconciliationFindingRepository.class})
    @Import(ReconciliationService.class)
    static class TestConfig {
    }

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("payment_platform")
            .withUsername("payment_app")
            .withPassword("payment_app");

    @DynamicPropertySource
    static void registerPostgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private ReconciliationFindingRepository reconciliationFindingRepository;

    @AfterEach
    void cleanUp() {
        reconciliationFindingRepository.deleteAll();
        ledgerEntryRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    private Payment insertCapturedPayment(String idempotencyKey) {
        Payment payment = new Payment("merchant-1", new BigDecimal("100.00"), "USD", idempotencyKey);
        payment.transitionTo(PaymentStatus.AUTHORIZED);
        payment.transitionTo(PaymentStatus.CAPTURED);
        return paymentRepository.save(payment);
    }

    private List<ReconciliationFinding> findingsFor(UUID paymentId) {
        return reconciliationFindingRepository.findAll().stream()
                .filter(finding -> finding.getPaymentId().equals(paymentId))
                .toList();
    }

    @Test
    void missingLedgerEntries_capturedPaymentWithNoEntries_detected() {
        Payment payment = insertCapturedPayment("key-missing-1");

        reconciliationService.runReconciliation();

        List<ReconciliationFinding> findings = findingsFor(payment.getId());
        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).getFindingType()).isEqualTo(FindingType.MISSING_LEDGER_ENTRIES);
    }

    @Test
    void unbalancedLedgerEntries_mismatchedAmounts_detected() {
        Payment payment = insertCapturedPayment("key-unbalanced-1");

        ledgerEntryRepository.save(new LedgerEntry(payment.getId(), LedgerAccount.PLATFORM_SUSPENSE,
                EntryType.DEBIT, new BigDecimal("100.00"), PaymentStatus.CAPTURED));
        ledgerEntryRepository.save(new LedgerEntry(payment.getId(), LedgerAccount.MERCHANT_WALLET,
                EntryType.CREDIT, new BigDecimal("50.00"), PaymentStatus.CAPTURED));

        reconciliationService.runReconciliation();

        List<ReconciliationFinding> findings = findingsFor(payment.getId());
        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).getFindingType()).isEqualTo(FindingType.UNBALANCED_LEDGER_ENTRIES);
        assertThat(findings.get(0).getDetail()).contains("100.00").contains("50.00");
    }

    @Test
    void healthyCapturedPayment_directInsertMatchingRealShape_noFindings() {
        Payment payment = insertCapturedPayment("key-healthy-1");

        // Matches LedgerEntry.pairFor's actual CAPTURED output shape: PLATFORM_SUSPENSE debit,
        // MERCHANT_WALLET credit, equal amounts.
        ledgerEntryRepository.saveAll(LedgerEntry.pairFor(payment.getId(), PaymentStatus.CAPTURED,
                payment.getAmount()));

        reconciliationService.runReconciliation();

        assertThat(findingsFor(payment.getId())).isEmpty();
    }
}
