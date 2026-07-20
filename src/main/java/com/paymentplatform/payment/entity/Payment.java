package com.paymentplatform.payment.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * @Version gives us optimistic locking for free: Hibernate checks this
 * column on every UPDATE and throws OptimisticLockException if another
 * transaction changed the row first. This is our first line of defense
 * against two concurrent events racing to update the same payment.
 *
 * Setters are package-private on purpose — status transitions must go
 * through the orchestration layer's transition logic, not be set directly
 * from anywhere in the codebase. (You'll build that transition method
 * next — see the TODO in orchestration package.)
 */
@Entity
@Table(name = "payments")
@Getter
@Setter(AccessLevel.PACKAGE)
@NoArgsConstructor
public class Payment {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(nullable = false, unique = true)
    private String idempotencyKey;

    @Version
    private Integer version;

    @CreationTimestamp
    private Instant createdAt;

    @UpdateTimestamp
    private Instant updatedAt;

    public Payment(String merchantId, BigDecimal amount, String currency, String idempotencyKey) {
        this.merchantId = merchantId;
        this.amount = amount;
        this.currency = currency;
        this.idempotencyKey = idempotencyKey;
        this.status = PaymentStatus.CREATED;
    }

    // TODO (you write this): a transitionTo(PaymentStatus next) method that
    // validates the transition against the legal-transitions map before
    // mutating status. This is the heart of the state machine — don't let
    // it just be `this.status = next` with no validation, or the "state
    // machine" is only a state machine in name.
}
