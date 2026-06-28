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
    //workers
    public static final String WORK_QUEUE = "work.queue";
    public static final String WORK_EXCHANGE = "work.exchange";
    public static final String WORK_ROUTING_KEY = "work.routing.key";

    //fan-out exchange
    public static final String FANOUT_EXCHANGE = "fanout.exchange";
    public static final String EMAIL_QUEUE = "email.queue";
    public static final String ANALYTICS_QUEUE = "analytics.queue";
    public static final String AUDIT_QUEUE = "audit.queue";

    //Hello queue config
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

    @Bean(name = "workerContainerFactory")
    public SimpleRabbitListenerContainerFactory workerContainerFactory(ConnectionFactory connectionFactory){
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(1);
        return factory;
    }

    //fanout confi
    @Bean
    public FanoutExchange fanoutExchange(){
        return new FanoutExchange(FANOUT_EXCHANGE);
    }

    @Bean
    public Queue emailQueue(){
        return new Queue(EMAIL_QUEUE, true);
    }
    @Bean
    public Queue analyticsQueue(){
        return new Queue(ANALYTICS_QUEUE, true);
    }
    @Bean
    public Queue auditQueue(){
        return new Queue(AUDIT_QUEUE, true);
    }
    @Bean
    public Binding emailBinding(){
        return BindingBuilder
                .bind(emailQueue())
                .to(fanoutExchange());
    }
    @Bean
    public Binding auditBinding(){
        return BindingBuilder
                .bind(auditQueue())
                .to(fanoutExchange());
    }
    @Bean
    public Binding analyticsBinding(){
        return BindingBuilder
                .bind(analyticsQueue())
                .to(fanoutExchange());
    }
}
