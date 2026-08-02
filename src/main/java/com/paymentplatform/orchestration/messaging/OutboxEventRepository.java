package com.paymentplatform.orchestration.messaging;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // Capped at 100 per query so a large unpublished backlog can't be pulled
    // into memory in one go — the relay simply picks it up again next cycle.
    List<OutboxEvent> findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
}
