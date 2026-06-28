package com.javaguy.springrabbitmq.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.*;

@Component
public class EventSubscribers {

    @RabbitListener(queues = EMAIL_QUEUE)
    public void onEmail(String event) {
        System.out.println("[Email Service    ] Sending welcome email for: " + event);
    }

    @RabbitListener(queues = ANALYTICS_QUEUE)
    public void onAnalytics(String event) {
        System.out.println("[Analytics Service] Logging signup event: " + event);
    }

    @RabbitListener(queues = AUDIT_QUEUE)
    public void onAudit(String event) {
        System.out.println("[Audit Service    ] Recording event: " + event);
    }
}
