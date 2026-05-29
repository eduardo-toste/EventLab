package com.project.eventlab.dto;

public record OrderResponse(

        String orderId,
        String status,
        String message

) {
}
