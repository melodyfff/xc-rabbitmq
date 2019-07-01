package com.xinchen.rabbitmq.tutorial.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 *
 * Work Queues
 *
 * 消费Queue中的消息
 * reference : https://www.rabbitmq.com/tutorials/tutorial-two-java.html
 *
 * 默认情况下，RabbitMQ将按顺序将每条消息发送给下一个消费者。
 * 平均而言，每个消费者将获得相同数量的消息。这种分发消息的方式称为循环法。
 * @author xinchen
 * @version 1.0
 * @date 01/07/2019 12:55
 */
@Slf4j
public class Worker1 {
    private static final String QUEUE_NAME = "hello";

    private static final Random random = new Random();

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // default port 5672
        factory.setPort(5672);
        // default user guest/guest
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 队列名,队列是否持久化,队列是否独占(仅次此连接),队列是否自动删除,构造参数
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        log.info(" [*] Waiting for messages. To exit press CTRL+C ");

        // 定义传输回调
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info(" [x] Received '{}'",message);
            try {
                doWork(message);
            } catch (InterruptedException e) {
                log.error("[x] Some thing wrong. {}",e);
            } finally {
                log.info(" [∨] Done");
            }
        };

        // 消费队列
        // 队列名称,是否autoAck,传递消息时的回调,取消消费时的回调

        boolean autoAck = true;

        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }


    private static void doWork(String task) throws InterruptedException {
        log.info(" [-] Handle message '{}'",task);
        Thread.sleep((random.nextInt(5))*10000);
    }

}
