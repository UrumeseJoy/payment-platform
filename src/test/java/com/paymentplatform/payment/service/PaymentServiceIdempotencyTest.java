package com.paymentplatform.payment.service;

import com.paymentplatform.payment.dto.CreatePaymentRequest;
import com.paymentplatform.payment.dto.PaymentResponse;
import com.paymentplatform.payment.repository.PaymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(properties = "spring.data.redis.repositories.enabled=false")
class PaymentServiceIdempotencyTest {

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
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    // Redis is not under test here, so we mock it out entirely rather than
    // spinning up a Redis Testcontainer — the idempotency guarantee we're
    // proving lives in the DB unique constraint, not the cache.
    @MockBean(name = "redisTemplate")
    private RedisTemplate<String, String> redisTemplate;

    @AfterEach
    void cleanUp() {
        paymentRepository.deleteAll();
    }

    private CreatePaymentRequest sampleRequest() {
        CreatePaymentRequest request = new CreatePaymentRequest();
        request.setMerchantId("merchant-123");
        request.setAmount(new BigDecimal("25.00"));
        request.setCurrency("USD");
        return request;
    }

    @Test
    void sequentialDuplicateRequestReturnsOriginalPayment() {
        ValueOperations<String, String> valueOperations = Mockito.mock(ValueOperations.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        String idempotencyKey = "seq-key-1";
        CreatePaymentRequest request = sampleRequest();

        PaymentResponse first = paymentService.createPayment(request, idempotencyKey);
        PaymentResponse second = paymentService.createPayment(request, idempotencyKey);

        assertThat(first.getId()).isEqualTo(second.getId());
        assertThat(paymentRepository.count()).isEqualTo(1);
    }

    @Test
    void concurrentDuplicateRequestsCreateExactlyOnePayment() throws InterruptedException {
        ValueOperations<String, String> valueOperations = Mockito.mock(ValueOperations.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        int threadCount = 10;
        String idempotencyKey = "concurrent-key-1";
        CreatePaymentRequest request = sampleRequest();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<PaymentResponse> responses = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    startLatch.await();
                    PaymentResponse response = paymentService.createPayment(request, idempotencyKey);
                    responses.add(response);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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

        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(responses).hasSize(threadCount);

        Set<Object> distinctIds = responses.stream()
                .map(PaymentResponse::getId)
                .collect(Collectors.toSet());
        assertThat(distinctIds).hasSize(1);
    }
}
