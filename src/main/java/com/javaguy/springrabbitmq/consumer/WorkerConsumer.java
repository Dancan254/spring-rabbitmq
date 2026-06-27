package com.javaguy.springrabbitmq.consumer;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.javaguy.springrabbitmq.config.RabbitMQConfig.WORK_QUEUE;
import static java.lang.IO.println;

@Component
public class WorkerConsumer {

    @RabbitListener(
            queues = WORK_QUEUE,
            containerFactory = "workerContainerFactory",
            concurrency = "2"
    )
    public void processTask(
            String task,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException, InterruptedException {

        String worker = Thread.currentThread().getName();
        System.out.println("[" + worker + "] Started : " + task);

        try {
            int dots = (int) task.chars().filter(c -> c == '.').count();
            Thread.sleep(dots * 1000L); // 1 second per dot
            System.out.println("[" + worker + "] Done    : " + task);
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            System.out.println("[" + worker + "] Failed  : " + task);
            channel.basicNack(deliveryTag, false, true); // requeue on unexpected failure
        }
    }
}
