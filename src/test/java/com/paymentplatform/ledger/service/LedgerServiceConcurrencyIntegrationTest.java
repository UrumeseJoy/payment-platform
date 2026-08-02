package com.paymentplatform.ledger.service;

import com.paymentplatform.ledger.entity.EntryType;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test deliberately uses a real Postgres container and real concurrent threads,
 * not mocks: the thing being verified — whether @Version-based optimistic locking plus
 * LedgerService's own retry loop actually prevent duplicate ledger postings — can only
 * be proven under genuine concurrent contention against a real database. A mocked
 * repository would never produce a real OptimisticLockingFailureException, so it would
 * prove nothing about the race this test exists to catch.
 *
 * @SpringBootTest loads the FULL application context by default, which would include
 * PaymentEventConsumer's @KafkaListener container and the auto-configured KafkaAdmin
 * bean — neither of which this test has any business depending on, since it only
 * exercises LedgerService against Postgres. Suppressing just the listener's
 * auto-startup wasn't enough: KafkaAdmin independently attempts to provision the
 * NewTopic bean from KafkaTopicConfig at context startup regardless of listener state,
 * which added a ~30-40s delay (and a latent failure risk under fail-fast admin
 * configs) when no broker was reachable. Simply excluding KafkaAutoConfiguration also
 * isn't enough on its own: PaymentEventProducer/PaymentEventConsumer are real @Service
 * beans that directly require a KafkaTemplate bean in their constructors, so removing
 * Kafka's auto-configuration without also removing those beans just trades a slow
 * failure for an immediate "no qualifying bean" one. Instead this test replaces
 * @SpringBootTest's default full-app context with the narrow TestConfig below — only
 * JPA/Postgres infrastructure plus LedgerService — so the Kafka-dependent beans are
 * never created in the first place and no Kafka broker is involved at all.
 */
@Testcontainers
@SpringBootTest(
        classes = LedgerServiceConcurrencyIntegrationTest.TestConfig.class,
        properties = "spring.data.redis.repositories.enabled=false")
class LedgerServiceConcurrencyIntegrationTest {

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

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Test
    void concurrentApplyTransition_sameTargetStatus_exactlyOnceEffect() throws InterruptedException {
        Payment newPayment = new Payment("merchant-1", new BigDecimal("50.00"), "USD", "key-concurrency-1");
        newPayment.transitionTo(PaymentStatus.AUTHORIZED);
        Payment payment = paymentRepository.save(newPayment);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<Exception> failures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    startLatch.await();
                    ledgerService.applyTransition(payment.getId(), PaymentStatus.CAPTURED);
                } catch (Exception e) {
                    failures.add(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();

        doneLatch.await();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        int succeeded = threadCount - failures.size();
        System.out.println("concurrentApplyTransition_sameTargetStatus_exactlyOnceEffect: "
                + succeeded + " of " + threadCount + " threads succeeded, "
                + failures.size() + " threw an exception (expected under real contention "
                + "once a thread's 3 internal retries are exhausted).");
        failures.forEach(e -> System.out.println("  thread exception: " + e));

        Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.CAPTURED);

        List<LedgerEntry> entries = ledgerEntryRepository.findByPaymentId(payment.getId());
        assertThat(entries).hasSize(2);

        long debitCount = entries.stream().filter(e -> e.getEntryType() == EntryType.DEBIT).count();
        long creditCount = entries.stream().filter(e -> e.getEntryType() == EntryType.CREDIT).count();
        assertThat(debitCount).isEqualTo(1);
        assertThat(creditCount).isEqualTo(1);

        BigDecimal debitAmount = entries.stream()
                .filter(e -> e.getEntryType() == EntryType.DEBIT)
                .findFirst().orElseThrow().getAmount();
        BigDecimal creditAmount = entries.stream()
                .filter(e -> e.getEntryType() == EntryType.CREDIT)
                .findFirst().orElseThrow().getAmount();
        assertThat(debitAmount).isEqualByComparingTo(creditAmount);
    }
}
