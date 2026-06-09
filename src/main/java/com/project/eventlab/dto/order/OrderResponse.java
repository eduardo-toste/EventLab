package com.project.eventlab.dto.order;

public record OrderResponse(

        String orderId,
        String status,
        String message

) {
}
