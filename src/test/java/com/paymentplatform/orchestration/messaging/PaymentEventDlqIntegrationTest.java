package com.paymentplatform.orchestration.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.payment.entity.Payment;
import com.paymentplatform.payment.entity.PaymentStatus;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DefaultErrorHandler invokes DeadLetterPublishingRecoverer via two structurally different code
 * paths: immediately, for exceptions classified as non-retryable (IllegalArgumentException, in
 * this codebase's dispatch-default branch), and only after the retry/backoff schedule is fully
 * exhausted, for anything else (GatewayTimeoutException). These aren't the same path with
 * different timing incidentally — they're different branches inside the error handler's
 * classification logic — so proving one says nothing about the other; both are covered here
 * separately, each reading back the actual record DeadLetterPublishingRecoverer published to
 * payment-events.DLT via a raw KafkaConsumer, not just asserting no exception was thrown.
 */
@Testcontainers
@SpringBootTest(properties = "spring.data.redis.repositories.enabled=false")
class PaymentEventDlqIntegrationTest {

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
    private static final String DLT_TOPIC = "payment-events.DLT";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaConsumer<String, String> dltConsumer;

    @AfterEach
    void cleanUp() {
        if (dltConsumer != null) {
            dltConsumer.close();
        }
        paymentRepository.deleteAll();
    }

    private KafkaConsumer<String, String> newDltConsumer(String groupIdSuffix) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-" + groupIdSuffix + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(DLT_TOPIC));
        return consumer;
    }

    // Every test's DLT consumer joins the SAME real payment-events.DLT topic (a fresh, unique
    // consumer group per test, but earliest-offset, so a new group replays the whole topic
    // history) — so records from earlier tests are still visible here. Filtering by the
    // expected key is required, not optional, or a later test can pick up an earlier test's
    // leftover DLT record instead of its own.
    private ConsumerRecord<String, String> pollForRecord(KafkaConsumer<String, String> consumer, Duration timeout,
                                                           String expectedKey) {
        AtomicReference<ConsumerRecord<String, String>> found = new AtomicReference<>();

        await().atMost(timeout).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                if (expectedKey.equals(record.key())) {
                    found.set(record);
                }
            }
            assertThat(found.get()).isNotNull();
        });

        return found.get();
    }

    @Test
    void unsupportedEventType_routesImmediatelyToDlq() {
        dltConsumer = newDltConsumer("unsupported-type");

        UUID paymentId = UUID.randomUUID();
        publish(new PaymentEvent(paymentId, PaymentEventType.PAYMENT_CAPTURED, new BigDecimal("10.00")));

        ConsumerRecord<String, String> dltRecord =
                pollForRecord(dltConsumer, Duration.ofSeconds(10), paymentId.toString());

        assertThat(dltRecord.key()).isEqualTo(paymentId.toString());
        // The @KafkaListener container wraps whatever the listener method throws in a
        // ListenerExecutionFailedException before DefaultErrorHandler/the recoverer ever see
        // it — that's the top-level exception header. The original exception this codebase
        // actually threw shows up as the CAUSE, in a separate header.
        assertThat(headerValue(dltRecord, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .isEqualTo("org.springframework.kafka.listener.ListenerExecutionFailedException");
        assertThat(headerValue(dltRecord, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo("java.lang.IllegalArgumentException");
    }

    @Test
    void gatewayTimeout_exhaustsRetriesThenRoutesToDlq() {
        dltConsumer = newDltConsumer("gateway-timeout");

        BigDecimal amount = new BigDecimal("100.99");
        Payment payment = paymentRepository.save(
                new Payment("merchant-1", amount, "USD", "key-dlq-timeout"));

        publish(new PaymentEvent(payment.getId(), PaymentEventType.PAYMENT_CREATED, amount));

        ConsumerRecord<String, String> dltRecord =
                pollForRecord(dltConsumer, Duration.ofSeconds(20), payment.getId().toString());

        assertThat(dltRecord.key()).isEqualTo(payment.getId().toString());
        assertThat(headerValue(dltRecord, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .isEqualTo("org.springframework.kafka.listener.ListenerExecutionFailedException");
        assertThat(headerValue(dltRecord, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo("com.paymentplatform.orchestration.messaging.GatewayTimeoutException");

        Payment reloaded = paymentRepository.findById(payment.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(PaymentStatus.CREATED);
    }

    private String headerValue(ConsumerRecord<String, String> record, String headerKey) {
        var header = record.headers().lastHeader(headerKey);
        assertThat(header).as("header " + headerKey).isNotNull();
        return new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
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
