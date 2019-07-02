package com.xinchen.rabbitmq.tutorial.publishsubscribe;

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
 * Publish/Subscribe
 *
 * reference: https://www.rabbitmq.com/tutorials/tutorial-three-java.html
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 11:41
 */
@Slf4j
public class EmitLog {
    private static final String EXCHANGE_NAME = "logs";

    private static final String MESSAGE = "%d Hello World!";

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        ConnectionFactory factory = CommonConnectionFactory.FACTORY;

        try (Connection connection = factory.newConnection()){
            Channel channel = connection.createChannel();
            // 设置exchange名称,交换类型可选 : [direct(直接) ,topic(主题) ,headers(标题) ,fanout(扇出)]
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            while (true){
                String message = String.format(MESSAGE, RANDOM.nextInt(10000));

                // 交换器名称,路由键,支持消息的其他属性-路由标题等,消息体
                channel.basicPublish(EXCHANGE_NAME,"",null,message.getBytes(StandardCharsets.UTF_8));
                log.info(" [∨] Sent '{}'",message);

                // 模拟延迟
                Thread.sleep(RANDOM.nextInt(3)*1000);
            }


        } catch (TimeoutException | IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
