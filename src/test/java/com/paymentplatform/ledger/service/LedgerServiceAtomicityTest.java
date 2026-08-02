package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.orchestration.messaging.OutboxEvent;
import com.paymentplatform.orchestration.messaging.OutboxEventRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

/**
 * LedgerServiceConcurrencyIntegrationTest proves the retry loop survives real contention, and
 * LedgerServiceRetryLogicTest proves the retry COUNT is exactly right against mocks — but
 * neither actually forces a mid-transaction failure and checks that the payment-status write
 * gets rolled back with it. That gap matters specifically because applyOnce's @Transactional
 * only became a real transaction (rather than a silently-ignored self-invocation) after the
 * outbox atomicity fix, which wired applyTransition to call applyOnce through a self-injected
 * proxy. This test exists to prove that fix actually delivers real rollback behavior, not just
 * correct retry counts: it forces the ledger-entry write to fail with a non-retryable exception
 * and checks that the payment's status write — which happens earlier in the same method — was
 * rolled back alongside it, rather than being left committed on its own.
 */
@Testcontainers
@SpringBootTest(
        classes = LedgerServiceAtomicityTest.TestConfig.class,
        properties = "spring.data.redis.repositories.enabled=false")
class LedgerServiceAtomicityTest {

    @Configuration
    @EnableAutoConfiguration
    @EntityScan(basePackageClasses = {Payment.class, LedgerEntry.class, OutboxEvent.class})
    @EnableJpaRepositories(basePackageClasses = {PaymentRepository.class, LedgerEntryRepository.class,
            OutboxEventRepository.class})
    @Import(LedgerService.class)
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
    private LedgerService ledgerService;

    @Autowired
    private PaymentRepository paymentRepository;

    // Real ledger writes are not under test here — this bean is mocked purely to inject a
    // deterministic, non-retryable failure at the exact point in applyOnce where the ledger
    // write happens, so we can observe whether the earlier payment-status write in the same
    // method survives that failure or gets rolled back with it.
    @MockBean
    private LedgerEntryRepository ledgerEntryRepository;

    @Test
    void ledgerWriteFailure_rollsBackPaymentStatusWrite() {
        when(ledgerEntryRepository.saveAll(anyList()))
                .thenThrow(new RuntimeException("simulated non-retryable ledger write failure"));

        Payment newPayment = new Payment("merchant-1", new BigDecimal("50.00"), "USD", "key-atomicity-1");
        newPayment.transitionTo(PaymentStatus.AUTHORIZED);
        Payment payment = paymentRepository.save(newPayment);

        assertThatThrownBy(() -> ledgerService.applyTransition(payment.getId(), PaymentStatus.CAPTURED))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("simulated non-retryable ledger write failure");

        Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
    }
}
