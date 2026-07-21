package com.paymentplatform.gateway;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

/**
 * A deterministic, test-mode payment gateway.
 *
 * Modeled after Stripe's test-card convention: rather than simulating
 * gateway behavior randomly, the outcome is derived entirely from the
 * cents value of the requested amount. This makes decline and timeout
 * scenarios reproducible on demand in tests and local development,
 * instead of being flaky or unobservable.
 *
 * Cents ending in 13 -> DECLINED, cents ending in 99 -> TIMEOUT,
 * anything else -> SUCCESS.
 */
@Service
public class MockPaymentGateway implements PaymentGateway {

    @Override
    public GatewayResult authorize(UUID paymentId, BigDecimal amount) {
        return resolveOutcome(amount);
    }

    @Override
    public GatewayResult capture(UUID paymentId, BigDecimal amount) {
        return resolveOutcome(amount);
    }

    private GatewayResult resolveOutcome(BigDecimal amount) {
        int cents = lastTwoCentsDigits(amount);

        return switch (cents) {
            case 13 -> new GatewayResult(GatewayOutcome.DECLINED, "Card declined by issuer");
            case 99 -> new GatewayResult(GatewayOutcome.TIMEOUT, "Gateway request timed out");
            default -> new GatewayResult(GatewayOutcome.SUCCESS, "Approved");
        };
    }

    private int lastTwoCentsDigits(BigDecimal amount) {
        BigDecimal cents = amount.remainder(BigDecimal.ONE)
                .abs()
                .setScale(2, RoundingMode.HALF_UP)
                .movePointRight(2);
        return cents.intValue();
    }
}
