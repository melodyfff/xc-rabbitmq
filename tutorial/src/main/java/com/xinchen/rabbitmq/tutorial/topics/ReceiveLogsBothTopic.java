package com.xinchen.rabbitmq.tutorial.topics;

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
 * Topics
 *
 * 主题订阅
 *
 * 全部处理的消费者
 *
 * reference: https://www.rabbitmq.com/tutorials/tutorial-five-java.html
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 14:31
 */
@Slf4j
public class ReceiveLogsBothTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        final ConnectionFactory factory = CommonConnectionFactory.FACTORY;
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        // exchangeName,exchangeType
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        // 生成临时队列
        String queueName = channel.queueDeclare().getQueue();


        // 绑定routingKey为anonymous.*的所有消息
        channel.queueBind(queueName, EXCHANGE_NAME, "anonymous.*");

        log.info(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info(" [x] Received  routingKey:message - {}:{}'",delivery.getEnvelope().getRoutingKey(),message);
        });


        channel.basicConsume(queueName,true,deliverCallback,consumerTag -> { });
    }
}
