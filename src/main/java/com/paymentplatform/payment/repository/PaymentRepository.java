package com.paymentplatform.payment.repository;

import com.paymentplatform.payment.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    // Backs the durable idempotency check: DB unique constraint on
    // idempotency_key is the source of truth, Redis is just a fast-path
    // cache in front of it.
    Optional<Payment> findByIdempotencyKey(String idempotencyKey);
}
