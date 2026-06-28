package com.javaguy.springrabbitmq.producer;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.FANOUT_EXCHANGE;
import static java.lang.IO.println;

@Component
public class EventPublisher {

    private final RabbitTemplate rabbitTemplate;

    public EventPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publish(String event) {
        println("[Publisher] Broadcasting: " + event);
        rabbitTemplate.convertAndSend(FANOUT_EXCHANGE, event);
    }
}
