package com.javaguy.springrabbitmq.producer;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.WORK_EXCHANGE;
import static com.javaguy.springrabbitmq.config.RabbitMQConfig.WORK_ROUTING_KEY;
import static java.lang.IO.println;

@Component
public class TaskProducer {

    private final RabbitTemplate template;

    public TaskProducer(RabbitTemplate template) {
        this.template = template;
    }

    public void dispatch(String task){
        println("[Producer] Dispatching task: " +  task);
        template.convertAndSend(WORK_EXCHANGE, WORK_ROUTING_KEY, task);
    }
}
