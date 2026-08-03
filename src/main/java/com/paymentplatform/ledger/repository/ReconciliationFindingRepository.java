package com.paymentplatform.ledger.repository;

import com.paymentplatform.ledger.entity.ReconciliationFinding;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ReconciliationFindingRepository extends JpaRepository<ReconciliationFinding, UUID> {
}
