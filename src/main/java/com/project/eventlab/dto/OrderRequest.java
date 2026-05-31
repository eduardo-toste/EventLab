package com.project.eventlab.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record OrderRequest(

        @NotBlank(message = "Customer id must not be blank")
        String customerId,

        @NotNull(message = "Total id must not be null")
        BigDecimal total

) {
}
