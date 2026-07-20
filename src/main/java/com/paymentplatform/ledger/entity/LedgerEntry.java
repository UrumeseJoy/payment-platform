package com.paymentplatform.ledger.entity;

import com.paymentplatform.payment.entity.PaymentStatus;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Deliberately immutable: no @Version, no updatable setters exposed, no
 * update methods at all. A ledger entry, once written, is never changed —
 * that's what makes it trustworthy as an audit trail. Reversing a payment
 * means writing NEW offsetting rows, never touching the old ones.
 */
@Entity
@Table(name = "ledger_entries")
@Getter
@Setter(AccessLevel.NONE)
@NoArgsConstructor
public class LedgerEntry {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private LedgerAccount account;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private EntryType entryType;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus relatedEvent;

    @CreationTimestamp
    private Instant createdAt;

    public LedgerEntry(UUID paymentId, LedgerAccount account, EntryType entryType,
                        BigDecimal amount, PaymentStatus relatedEvent) {
        this.paymentId = paymentId;
        this.account = account;
        this.entryType = entryType;
        this.amount = amount;
        this.relatedEvent = relatedEvent;
    }

    // TODO (you write this): a static factory like
    // LedgerEntry.pairFor(paymentId, PaymentStatus.CAPTURED, amount)
    // that returns the matched debit+credit pair for a given transition,
    // so callers can never accidentally post an unbalanced single entry.
}
