package com.javaguy.springrabbitmq.producer;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.TOPIC_EXCHANGE;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;


@Component
public class AppLogPublisher {
    private final RabbitTemplate rabbitTemplate;

    public AppLogPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publish(String message, String routingKey) {
        IO.println("[Publisher] key = " + routingKey + ", message = " + message);
        rabbitTemplate.convertAndSend(TOPIC_EXCHANGE, routingKey, message);
    }
}
