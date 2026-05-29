package com.project.eventlab.dto;

import java.math.BigDecimal;

public record OrderCreatedData(

        String orderId,
        String customerId,
        BigDecimal total

) {
}
