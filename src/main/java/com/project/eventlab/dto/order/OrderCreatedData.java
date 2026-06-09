package com.project.eventlab.dto.order;

import java.math.BigDecimal;

public record OrderCreatedData(

        String orderId,
        String customerId,
        BigDecimal total

) {
}
