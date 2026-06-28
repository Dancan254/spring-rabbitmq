package com.javaguy.springrabbitmq.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.*;
import static java.lang.IO.println;

@Component
public class EventSubscribers {

    @RabbitListener(queues = EMAIL_QUEUE)
    public void onEmail(String event) {
        println("[Email Service    ] Sending welcome email for: " + event);
    }

    @RabbitListener(queues = ANALYTICS_QUEUE)
    public void onAnalytics(String event) {
        println("[Analytics Service] Logging signup event: " + event);
    }

    @RabbitListener(queues = AUDIT_QUEUE)
    public void onAudit(String event) {
        println("[Audit Service    ] Recording event: " + event);
    }
}
