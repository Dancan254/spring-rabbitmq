package com.javaguy.springrabbitmq.producer;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.EXCHANGE_NAME;
import static com.javaguy.springrabbitmq.config.RabbitMQConfig.ROUTING_KEY;
import static java.lang.IO.println;

@Component
public class HelloProducer {
    private final RabbitTemplate rabbitTemplate;

    public HelloProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }
    public void send(String message){
        println("Sending: " + message); //should have used sl4j but just keeping it simple
        rabbitTemplate.convertAndSend(
                EXCHANGE_NAME, //this is a static import from the @RabbitMQConfig file
                ROUTING_KEY,
                message
        );
    }
}
