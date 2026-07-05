package com.javaguy.springrabbitmq.consumer;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.RELIABLE_QUEUE;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class ReliableOrderConsumer {

    @RabbitListener(queues = RELIABLE_QUEUE)
    public void onOrder(String message) {
        IO.println("[Order Service] received: " + message);
    }
}
