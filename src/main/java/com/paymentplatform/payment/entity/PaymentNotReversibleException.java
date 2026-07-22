package com.paymentplatform.payment.entity;

public class PaymentNotReversibleException extends RuntimeException {

    public PaymentNotReversibleException(PaymentStatus currentStatus) {
        super("Cannot reverse payment in status " + currentStatus
                + ": only CAPTURED payments can be reversed.");
    }
}
