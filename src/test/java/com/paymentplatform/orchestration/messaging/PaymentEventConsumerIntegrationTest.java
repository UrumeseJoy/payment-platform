package com.paymentplatform.orchestration.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.ledger.entity.EntryType;
import com.paymentplatform.ledger.entity.LedgerEntry;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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

/**
 * This flow only proves itself if the message actually crosses a real broker and comes back
 * out through real JSON deserialization and the real @KafkaListener dispatch — mocking any of
 * that would just be re-testing PaymentTest/LedgerEntryTest's business logic under a different
 * name, not the wiring/async path this class exists to verify.
 */
@Testcontainers
@SpringBootTest(properties = "spring.data.redis.repositories.enabled=false")
class PaymentEventConsumerIntegrationTest {

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
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String TOPIC = "payment-events";

    @AfterEach
    void cleanUp() {
        ledgerEntryRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    @Test
    void fullCascade_createdEvent_reachesCaptured_withBalancedLedger() {
        BigDecimal amount = new BigDecimal("100.00");
        Payment payment = paymentRepository.save(
                new Payment("merchant-1", amount, "USD", "key-cascade-1"));

        publish(new PaymentEvent(payment.getId(), PaymentEventType.PAYMENT_CREATED, amount));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.CAPTURED);
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
    void createdEvent_declinedAmount_transitionsToFailed() {
        BigDecimal amount = new BigDecimal("100.13");
        Payment payment = paymentRepository.save(
                new Payment("merchant-1", amount, "USD", "key-declined-created"));

        publish(new PaymentEvent(payment.getId(), PaymentEventType.PAYMENT_CREATED, amount));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.FAILED);
        });

        assertThat(ledgerEntryRepository.findByPaymentId(payment.getId())).isEmpty();
    }

    @Test
    void authorizedEvent_declinedAmount_transitionsToFailed() {
        BigDecimal amount = new BigDecimal("100.13");
        Payment newPayment = new Payment("merchant-1", amount, "USD", "key-declined-authorized");
        newPayment.transitionTo(PaymentStatus.AUTHORIZED);
        Payment payment = paymentRepository.save(newPayment);

        publish(new PaymentEvent(payment.getId(), PaymentEventType.PAYMENT_AUTHORIZED, amount));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.FAILED);
        });

        assertThat(ledgerEntryRepository.findByPaymentId(payment.getId())).isEmpty();
    }

    private void publish(PaymentEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(TOPIC, event.paymentId().toString(), json);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish test event", e);
        }
    }
}
