package com.javaguy.springrabbitmq.producer;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.RELIABLE_EXCHANGE;

import java.util.UUID;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class ReliablePublisher {
    private final RabbitTemplate rabbitTemplate;

    public ReliablePublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publish(String routingKey, String message) {
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        IO.println("[Publisher] sending id=" + correlationData.getId() + " key=" + routingKey);
        rabbitTemplate.convertAndSend(RELIABLE_EXCHANGE, routingKey, message, correlationData);
    }
}
