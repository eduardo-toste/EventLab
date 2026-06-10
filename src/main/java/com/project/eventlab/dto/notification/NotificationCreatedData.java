package com.project.eventlab.dto.notification;

import com.project.eventlab.enums.NotificationChannel;

public record NotificationCreatedData(

        String notificationId,
        String orderId,
        String customerId,
        NotificationChannel channel,
        String message,
        String status

) {
}
