package com.project.eventlab.dto;

import java.math.BigDecimal;

public record OrderRequest(

        String customerId,
        BigDecimal total

) {
}
