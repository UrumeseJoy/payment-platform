package com.paymentplatform.ledger.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

/**
 * Append-only, same as LedgerEntry: a finding, once recorded, is never updated or deleted by
 * the reconciliation job itself — it's a record that a discrepancy was observed at a point in
 * time, not a live status to be flipped once "fixed" elsewhere.
 */
@Entity
@Table(name = "reconciliation_findings")
@Getter
@Setter(AccessLevel.NONE)
@NoArgsConstructor
public class ReconciliationFinding {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Enumerated(EnumType.STRING)
    @Column(name = "finding_type", nullable = false, length = 40)
    private FindingType findingType;

    @Column(nullable = false)
    private String detail;

    @CreationTimestamp
    @Column(name = "detected_at")
    private Instant detectedAt;

    public ReconciliationFinding(UUID paymentId, FindingType findingType, String detail) {
        this.paymentId = paymentId;
        this.findingType = findingType;
        this.detail = detail;
    }
}
