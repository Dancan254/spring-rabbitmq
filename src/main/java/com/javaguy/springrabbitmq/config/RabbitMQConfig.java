package com.javaguy.springrabbitmq.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    public static final String QUEUE_NAME = "hello.queue";
    public static final String EXCHANGE_NAME = "hello.exchange";
    public static final String ROUTING_KEY = "hello.routing.key";
    public static final String WORK_QUEUE = "work.queue";
    public static final String WORK_EXCHANGE = "work.exchange";
    public static final String WORK_ROUTING_KEY = "work.routing.key";

    @Bean
    public Queue queue(){
        return new Queue(QUEUE_NAME, true); // creates a durable queue
    }
    @Bean
    public DirectExchange exchange(){
        return new DirectExchange(EXCHANGE_NAME);
    }

    @Bean
    public Binding binding(Queue queue, DirectExchange exchange){
        return BindingBuilder
                .bind(queue)
                .to(exchange)
                .with(ROUTING_KEY);
    }

    // Work queue config
    @Bean
    public Queue workQueue(){
        return new Queue(WORK_QUEUE, true);
    }

    @Bean
    public DirectExchange workExchange(){
        return new DirectExchange(WORK_EXCHANGE);
    }

    @Bean
    public Binding workBinding(){
        return BindingBuilder
                .bind(workQueue())
                .to(workExchange())
                .with(WORK_ROUTING_KEY);
    }

    @Bean
    public SimpleRabbitListenerContainerFactory workerContainerFactory(ConnectionFactory connectionFactory){
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(1);
        return factory;
    }


}
