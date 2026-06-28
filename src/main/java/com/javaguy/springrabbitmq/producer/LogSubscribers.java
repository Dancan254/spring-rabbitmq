package com.javaguy.springrabbitmq.producer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.ALERT_QUEUE;
import static com.javaguy.springrabbitmq.config.RabbitMQConfig.LOG_QUEUE;
import static java.lang.IO.println;

@Component
public class LogSubscribers {

    @RabbitListener(queues = LOG_QUEUE)
    public void onLog(String message) {
        println("[Log Store  ] Persisting  : " + message);
    }

    @RabbitListener(queues = ALERT_QUEUE)
    public void onAlert(String message) {
        println("[Alert      ] Paging on-call for: " + message);
    }
}
