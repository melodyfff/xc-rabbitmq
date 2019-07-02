package com.xinchen.rabbitmq.tutorial.publishsubscribe;

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
 * Publish/Subscribe
 *
 * reference: https://www.rabbitmq.com/tutorials/tutorial-three-java.html
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 12:44
 */
@Slf4j
public class ReceiveLogs2 {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = CommonConnectionFactory.FACTORY;

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 设置exchange名称,交换类型可选 : [direct(直接) ,topic(主题) ,headers(标题) ,fanout(扇出/广播)]
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        // 临时队列: 当我们没有向queueDeclare()提供参数时，我们 使用生成的名称创建一个非持久的，独占的自动删除队列
        String queueName = channel.queueDeclare().getQueue();

        // 绑定exchange和queue
        // 参数: queue ,echange ,routingKey
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        log.info(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info(" [-] Received '{}'",message);
        });

        // 队列名称,是否autoAck,传递消息时的回调,取消消费时的回调
        channel.basicConsume(queueName,true,deliverCallback,consumerTag -> { });
    }
}
