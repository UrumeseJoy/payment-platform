package com.paymentplatform.payment.entity;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentTest {

    @Test
    void capturedToSettled_isLegal() {
        Payment payment = new Payment("merchant-1", new BigDecimal("10.00"), "USD", "key-1");
        payment.transitionTo(PaymentStatus.AUTHORIZED);
        payment.transitionTo(PaymentStatus.CAPTURED);
        payment.transitionTo(PaymentStatus.SETTLED);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.SETTLED);
    }

    @Test
    void createdToSettled_throwsException() {
        Payment payment = new Payment("merchant-1", new BigDecimal("10.00"), "USD", "key-2");
        assertThrows(IllegalStateTransitionException.class,
                () -> payment.transitionTo(PaymentStatus.SETTLED));
    }
}
