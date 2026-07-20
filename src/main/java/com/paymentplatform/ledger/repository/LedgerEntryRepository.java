package com.paymentplatform.ledger.repository;

import com.paymentplatform.ledger.entity.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    // Used by the reconciliation job (Tier 2) to sum debits/credits per
    // payment and confirm they net to zero.
    List<LedgerEntry> findByPaymentId(UUID paymentId);
}
