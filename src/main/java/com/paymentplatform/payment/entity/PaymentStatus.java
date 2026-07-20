package com.paymentplatform.payment.entity;

/**
 * The payment lifecycle, locked in Day 1 design:
 *
 *   CREATED --> AUTHORIZED --> CAPTURED --> SETTLED
 *      |             |             |
 *      v             v             v
 *   FAILED        FAILED       REVERSED
 *
 * REVERSED is only reachable from CAPTURED in v1 — see README design
 * decisions for why we didn't split this into VOID vs REFUND yet.
 *
 * NOTE: legal-transition validation lives in the orchestration layer
 * (this is intentionally just the set of possible values, not the rules).
 */
public enum PaymentStatus {
    CREATED,
    AUTHORIZED,
    CAPTURED,
    SETTLED,
    FAILED,
    REVERSED
}
