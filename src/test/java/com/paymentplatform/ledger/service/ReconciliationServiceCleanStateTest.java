package com.paymentplatform.ledger.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.ledger.entity.ReconciliationFinding;
import com.paymentplatform.ledger.repository.LedgerEntryRepository;
import com.paymentplatform.ledger.repository.ReconciliationFindingRepository;
import com.paymentplatform.orchestration.messaging.PaymentEvent;
import com.paymentplatform.orchestration.messaging.PaymentEventType;
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
 * Unlike ReconciliationServiceIntegrationTest (hand-shaped repository data, no Kafka),
 * this class drives a payment through the REAL production write path — Kafka ->
 * PaymentEventConsumer -> MockPaymentGateway -> LedgerService — and only then runs
 * reconciliation against whatever that real path actually wrote. It needs the full app
 * context with a real Kafka broker for that reason, so it lives in its own file rather than
 * mixing context requirements with the sliced-context detection-rule tests.
 */
@Testcontainers
@SpringBootTest(properties = "spring.data.redis.repositories.enabled=false")
class ReconciliationServiceCleanStateTest {

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

    private static final String TOPIC = "payment-events";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private ReconciliationFindingRepository reconciliationFindingRepository;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private ObjectMapper objectMapper;

    @AfterEach
    void cleanUp() {
        reconciliationFindingRepository.deleteAll();
        ledgerEntryRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    @Test
    void realPipelineCapturedPayment_noFindings() {
        BigDecimal amount = new BigDecimal("100.00");
        Payment payment = paymentRepository.save(
                new Payment("merchant-1", amount, "USD", "key-clean-state-1"));

        publish(new PaymentEvent(payment.getId(), PaymentEventType.PAYMENT_CREATED, amount));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.CAPTURED);
        });

        reconciliationService.runReconciliation();

        List<ReconciliationFinding> findings = reconciliationFindingRepository.findAll().stream()
                .filter(finding -> finding.getPaymentId().equals(payment.getId()))
                .toList();
        assertThat(findings).isEmpty();
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
