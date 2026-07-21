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
import java.util.List;
import java.util.Set;
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

    private static final Set<PaymentStatus> LEDGER_AFFECTING_EVENTS =
            Set.of(PaymentStatus.CAPTURED, PaymentStatus.REVERSED);

    public static boolean affectsLedger(PaymentStatus event) {
        return LEDGER_AFFECTING_EVENTS.contains(event);
    }

    public static List<LedgerEntry> pairFor(UUID paymentId, PaymentStatus event, BigDecimal amount) {
        return switch (event) {
            case CAPTURED -> List.of(
                    new LedgerEntry(paymentId, LedgerAccount.PLATFORM_SUSPENSE, EntryType.DEBIT, amount, event),
                    new LedgerEntry(paymentId, LedgerAccount.MERCHANT_WALLET, EntryType.CREDIT, amount, event)
            );
            case REVERSED -> List.of(
                    new LedgerEntry(paymentId, LedgerAccount.MERCHANT_WALLET, EntryType.DEBIT, amount, event),
                    new LedgerEntry(paymentId, LedgerAccount.CUSTOMER_WALLET, EntryType.CREDIT, amount, event)
            );
            default -> throw new IllegalArgumentException(
                    "No ledger postings are defined for event: " + event);
        };
    }
}
