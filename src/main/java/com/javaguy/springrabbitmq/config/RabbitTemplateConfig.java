package com.javaguy.springrabbitmq.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitTemplateConfig {

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);

        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            String id = correlationData != null ? correlationData.getId() : "Unknown";
            if (ack) {
                IO.println("[Confirm] ACK  id=" + id + " — broker stored the message");
            } else {
                IO.println("[Confirm] NACK id=" + id + " — broker rejected it: " + cause);
            }
        });

        rabbitTemplate.setReturnsCallback(returned -> {
            IO.println("[Return ] UNROUTABLE key=" + returned.getRoutingKey()
                    + " reply=" + returned.getReplyText()
                    + " body=" + new String(returned.getMessage().getBody()));
        });
        rabbitTemplate.setMandatory(true);
        return rabbitTemplate;
    }
}
