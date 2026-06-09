package com.project.eventlab.dto.payment;

import com.project.eventlab.enums.PaymentStatus;

import java.math.BigDecimal;

public record PaymentProcessedData(

        String paymentId,
        String orderId,
        String customerId,
        BigDecimal amount,
        PaymentStatus status,
        String reason

) {
}
