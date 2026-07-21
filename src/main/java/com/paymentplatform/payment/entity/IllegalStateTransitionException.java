package com.paymentplatform.payment.entity;

public class IllegalStateTransitionException extends RuntimeException {

    private final PaymentStatus from;
    private final PaymentStatus to;

    public IllegalStateTransitionException(PaymentStatus from, PaymentStatus to) {
        super("Cannot transition payment from " + from + " to " + to);
        this.from = from;
        this.to = to;
    }

    public PaymentStatus getFrom() {
        return from;
    }

    public PaymentStatus getTo() {
        return to;
    }
}
