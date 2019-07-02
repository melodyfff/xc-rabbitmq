package com.xinchen.rabbitmq.tutorial.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.xinchen.rabbitmq.tutorial.CommonConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 *
 * Routing
 *
 * <p>
 * 处理奇数的消息队列
 * <p>
 *
 * reference: https://www.rabbitmq.com/tutorials/tutorial-four-java.html
 *
 *
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 13:53
 */
@Slf4j
public class ReceiveLogsOddDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        final ConnectionFactory factory = CommonConnectionFactory.FACTORY;
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();


        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();
        // 绑定处理奇数
        channel.queueBind(queueName, EXCHANGE_NAME, "odd");

        log.info(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info(" [x] Received  routingKey:message - {}:{}'",delivery.getEnvelope().getRoutingKey(),message);
        });

        // 队列名称,是否autoAck,传递消息时的回调,取消消费时的回调
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
