package com.paymentplatform.gateway;

import java.math.BigDecimal;
import java.util.UUID;

public interface PaymentGateway {

    GatewayResult authorize(UUID paymentId, BigDecimal amount);

    GatewayResult capture(UUID paymentId, BigDecimal amount);
}
