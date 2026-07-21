package com.paymentplatform.ledger.entity;

import com.paymentplatform.payment.entity.PaymentStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LedgerEntryTest {

    @Test
    void capturedPairBalances() {
        UUID paymentId = UUID.randomUUID();

        List<LedgerEntry> pair = LedgerEntry.pairFor(
                paymentId,
                PaymentStatus.CAPTURED,
                new BigDecimal("50.00")
        );

        assertThat(pair).hasSize(2);

        BigDecimal debits = pair.stream()
                .filter(e -> e.getEntryType() == EntryType.DEBIT)
                .map(LedgerEntry::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        BigDecimal credits = pair.stream()
                .filter(e -> e.getEntryType() == EntryType.CREDIT)
                .map(LedgerEntry::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        assertThat(debits).isEqualByComparingTo(credits);
    }

    @Test
    void authorizedEventThrows() {
        assertThrows(
                IllegalArgumentException.class,
                () -> LedgerEntry.pairFor(
                        UUID.randomUUID(),
                        PaymentStatus.AUTHORIZED,
                        new BigDecimal("50.00")
                )
        );
    }
}