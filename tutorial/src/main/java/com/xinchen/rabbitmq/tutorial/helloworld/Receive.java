package com.xinchen.rabbitmq.tutorial.helloworld;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 消费Queue中的消息
 * reference : https://www.rabbitmq.com/tutorials/tutorial-one-java.html
 * @author xinchen
 * @version 1.0
 * @date 01/07/2019 12:55
 */
@Slf4j
public class Receive {
    private static final String QUEUE_NAME = "hello";

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
        };

        // 消费队列
        // 队列名称,是否autoAck,传递消息时的回调,取消消费时的回调
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    }

}
