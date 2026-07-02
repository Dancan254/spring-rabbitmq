package com.javaguy.springrabbitmq.consumer;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.ALL_LOGS_QUEUE;
import static com.javaguy.springrabbitmq.config.RabbitMQConfig.CRITICAL_QUEUE;
import static com.javaguy.springrabbitmq.config.RabbitMQConfig.PAYMENT_QUEUE;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class AppLogConsumer {

    @RabbitListener(queues = CRITICAL_QUEUE)
    public void onCritical(String message){
        IO.println("[Alert Service] Error detected " + message);
    }

    @RabbitListener(queues = PAYMENT_QUEUE)
    public void onPayment(String message){
        IO.println("[Payment Service] Payment log " + message);
    }

    @RabbitListener(queues = ALL_LOGS_QUEUE)
    public void onAllLogs(String message){
        IO.println("[Log Aggregator] Log " + message);
    }
    
}
