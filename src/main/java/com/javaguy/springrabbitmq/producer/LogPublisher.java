package com.javaguy.springrabbitmq.producer;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.LOGS_EXCHANGE;
import static java.lang.IO.println;

@Component
public class LogPublisher {

    private final RabbitTemplate rabbitTemplate;

    public LogPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void log(String level, String message) {
        println("[Publisher] " + level.toUpperCase() + " — " + message);
        rabbitTemplate.convertAndSend(LOGS_EXCHANGE, level, message);
    }
}
