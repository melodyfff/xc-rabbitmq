package com.xinchen.rabbitmq.tutorial.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.xinchen.rabbitmq.tutorial.CommonConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Routing
 * <p>
 * 模拟区分奇数和偶数,分发给不同的消息队列处理
 * <p>
 * reference: https://www.rabbitmq.com/tutorials/tutorial-four-java.html
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 13:32
 */
@Slf4j
public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    private static final String MESSAGE = "%d Hello World!";

    /**
     * 偶数/奇数
     */
    private static final String[] SEVERITIES = {"even", "odd"};

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        final ConnectionFactory factory = CommonConnectionFactory.FACTORY;
        try (Connection connection = factory.newConnection()) {

            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");


            while (true) {
                int number = RANDOM.nextInt(10);
                // routingKey
                // 奇数/偶数的 routingKey不一样会交给不同的消息队列处理
                String severity = SEVERITIES[number % 2];
                String message = String.format(MESSAGE, number);

                // 交换器名称,路由键,支持消息的其他属性-路由标题等,消息体
                channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes(StandardCharsets.UTF_8));
                log.info(" [∨] Sent routingKey:message - {}:{}", severity,message);

                // 模拟业务延迟
                Thread.sleep(number * 1000);
            }


        } catch (TimeoutException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
