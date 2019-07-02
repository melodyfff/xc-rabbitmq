package com.xinchen.rabbitmq.tutorial.topics;

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
 *
 * Topics
 *
 * 主题订阅
 *
 * reference: https://www.rabbitmq.com/tutorials/tutorial-five-java.html
 *
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 14:19
 */
@Slf4j
public class EmitLogTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    private static final String MESSAGE = "%d Hello World!";

    /**
     * 偶数/奇数
     */
    private static final String[] ROUTING_KEY = {"anonymous.even", "anonymous.odd"};

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        final ConnectionFactory factory = CommonConnectionFactory.FACTORY;
        try (Connection connection = factory.newConnection()){

            final Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            while (true){
                int number = RANDOM.nextInt(10);

                // 随机分发奇数或者偶数的routingKey
                String routingKey = ROUTING_KEY[number%2];

                String message = String.format(MESSAGE,number);

                channel.basicPublish(EXCHANGE_NAME,routingKey,null,message.getBytes(StandardCharsets.UTF_8));

                log.info(" [∨] Sent routingKey:message - {}:{}", routingKey,message);
                Thread.sleep(number*1000);
            }
        } catch (TimeoutException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
