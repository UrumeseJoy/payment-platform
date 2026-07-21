package com.paymentplatform.orchestration.messaging;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentEvent(UUID paymentId, PaymentEventType eventType, BigDecimal amount) {
}
