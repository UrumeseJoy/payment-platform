package com.paymentplatform.ledger.entity;

/**
 * Fixed set of ledger accounts for v1. PLATFORM_SUSPENSE holds funds
 * mid-flight between authorization and capture/reversal so that every
 * intermediate state still nets to zero across the ledger.
 */
public enum LedgerAccount {
    CUSTOMER_WALLET,
    MERCHANT_WALLET,
    PLATFORM_SUSPENSE
}
