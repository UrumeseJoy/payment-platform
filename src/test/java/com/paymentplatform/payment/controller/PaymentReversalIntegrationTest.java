package com.paymentplatform.payment.controller;

import com.paymentplatform.ledger.entity.EntryType;
import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * This test needs the FULL application context — controller, real Kafka, real Postgres —
 * because it exists to prove the entire async chain a real client's reversal request takes:
 * HTTP request -> PaymentService validation -> event published -> real Kafka round-trip ->
 * PaymentEventConsumer -> LedgerService -> final DB state. PaymentServiceIdempotencyTest,
 * LedgerServiceConcurrencyIntegrationTest, LedgerServiceRetryLogicTest, and
 * PaymentEventConsumerIntegrationTest already cover each of those components individually;
 * this class is deliberately the only one proving they're wired together correctly end to end.
 */
@Testcontainers
@SpringBootTest(properties = "spring.data.redis.repositories.enabled=false")
@AutoConfigureMockMvc
class PaymentReversalIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("payment_platform")
            .withUsername("payment_app")
            .withPassword("payment_app");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.producer.key-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.kafka.producer.value-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.kafka.consumer.key-deserializer",
                () -> "org.apache.kafka.common.serialization.StringDeserializer");
        registry.add("spring.kafka.consumer.value-deserializer",
                () -> "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @AfterEach
    void cleanUp() {
        ledgerEntryRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    @Test
    void reversePayment_capturedPayment_eventuallyReversedWithBalancedLedger() throws Exception {
        Payment newPayment = new Payment("merchant-1", new BigDecimal("75.00"), "USD", "key-reversal-1");
        newPayment.transitionTo(PaymentStatus.AUTHORIZED);
        newPayment.transitionTo(PaymentStatus.CAPTURED);
        Payment payment = paymentRepository.save(newPayment);

        mockMvc.perform(post("/payments/{id}/reverse", payment.getId()))
                .andExpect(status().isAccepted())
                .andExpect(content().string(""));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.REVERSED);
        });

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

    @Test
    void reversePayment_nonCapturedPayment_returns409Immediately() throws Exception {
        Payment payment = paymentRepository.save(
                new Payment("merchant-1", new BigDecimal("30.00"), "USD", "key-reversal-2"));

        mockMvc.perform(post("/payments/{id}/reverse", payment.getId()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value(
                        "Cannot reverse payment in status CREATED: only CAPTURED payments can be reversed."));
    }
}
