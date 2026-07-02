package com.javaguy.springrabbitmq.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    //Hello
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

    //Routing
    public static final String LOGS_EXCHANGE  = "logs.exchange";
    public static final String LOG_QUEUE      = "logs.all.queue";
    public static final String ALERT_QUEUE    = "logs.alert.queue";

    public static final String KEY_ERROR   = "error";
    public static final String KEY_WARNING = "warning";
    public static final String KEY_INFO    = "info";

    //Topic Exchange
    public static final String TOPIC_EXCHANGE   = "app.logs.exchange";
    public static final String CRITICAL_QUEUE   = "logs.critical.queue";
    public static final String PAYMENT_QUEUE    = "logs.payment.queue";
    public static final String ALL_LOGS_QUEUE   = "logs.all.queue";

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

    //Routing
    @Bean
    public DirectExchange logsExchange(){
        return new DirectExchange(LOGS_EXCHANGE);
    }
    @Bean
    public Queue logQueue(){
        return new Queue(LOG_QUEUE, true);
    }
    @Bean
    public Queue alertQueue(){
        return new Queue(ALERT_QUEUE, true);
    }
    //logs.all.queue
    @Bean
    public Binding logBindingError(){
        return BindingBuilder
                .bind(logQueue())
                .to(logsExchange())
                .with(KEY_ERROR);
    }

    @Bean
    public Binding logBindingWarning(){
        return BindingBuilder
                .bind(logQueue())
                .to(logsExchange())
                .with(KEY_WARNING);
    }

    @Bean
    public Binding logBindingInfo(){
        return BindingBuilder
                .bind(logQueue())
                .to(logsExchange())
                .with(KEY_INFO);
    }

    //logs.aletrt
    @Bean
    public Binding alertBindingError(){
        return BindingBuilder
                .bind(alertQueue())
                .to(logsExchange())
                .with(KEY_ERROR);
    }

    //Topic Exchange
    @Bean
    public TopicExchange topicExchange(){
        return new TopicExchange(TOPIC_EXCHANGE);
    }

    @Bean
    public Queue criticalQueue(){
        return new Queue(CRITICAL_QUEUE, true);
    }

    @Bean
    public Queue paymentQueue(){
        return new Queue(PAYMENT_QUEUE, true);
    }

    @Bean
    public Queue allLogsQueue(){
        return new Queue(ALL_LOGS_QUEUE, true);
    }

    //all errors, any service: *.error or #.error

    @Bean
    public Binding criticalBinding(){
        return BindingBuilder
                .bind(criticalQueue())
                .to(topicExchange())
                .with("#.critical");
    }

    //All payment logs, any level
    @Bean
    public Binding paymentBinding(){
        return BindingBuilder
                .bind(paymentQueue())
                .to(topicExchange())
                .with("payment.#");
    }
    // Everything: #
@Bean
public Binding allLogsBinding() {
    return BindingBuilder
                .bind(allLogsQueue())
                .to(topicExchange()).with("#");
}
}
