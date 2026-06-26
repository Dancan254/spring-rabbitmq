package com.javaguy.springrabbitmq.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.QUEUE_NAME;
import static java.lang.IO.println;

@Component
public class HelloConsumer {

    @RabbitListener(queues = QUEUE_NAME)
    public void receive(String message) throws InterruptedException {
        Thread.sleep(5000);
        println("Received: " + message);
    }
}
