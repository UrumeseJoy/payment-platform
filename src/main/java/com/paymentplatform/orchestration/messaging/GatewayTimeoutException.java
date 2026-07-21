package com.paymentplatform.orchestration.messaging;

public class GatewayTimeoutException extends RuntimeException {

    public GatewayTimeoutException(String message) {
        super(message);
    }
}
