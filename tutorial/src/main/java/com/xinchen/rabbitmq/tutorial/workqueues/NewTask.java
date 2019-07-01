package com.xinchen.rabbitmq.tutorial.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 *
 * Work Queues
 *
 * 发送消息到Queue中
 * reference : https://www.rabbitmq.com/tutorials/tutorial-two-java.html
 *
 * @author xinchen
 * @version 1.0
 * @date 01/07/2019 11:37
 */
@Slf4j
public class NewTask {
    private static final String QUEUE_NAME = "hello_durable";

    private static final String[] MESSAGE = {"Hello", "World", "Hello World!"};

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // default port 5672
        factory.setPort(5672);
        // default user guest/guest
        factory.setUsername("guest");
        factory.setPassword("guest");

        try (Connection connection = factory.newConnection()){
            // 创建频道
            Channel channel = connection.createChannel();
            // 队列名,队列是否持久化,队列是否独占(仅次此连接),队列是否自动删除,构造参数
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String message = String.join("",randomMessage());

            // 发布消息
            // 交换器名称,路由键,支持消息的其他属性-路由标题等,消息体
            channel.basicPublish("",QUEUE_NAME,null,message.getBytes());
            log.info(" [∨] Sent '{}'",message);
        } catch (TimeoutException | IOException e) {
            log.error("[x] NewTask Error: {}",e);
        }
    }

    /**
     * 模拟随机返回一个消息
     * @return String
     */
    private static String randomMessage(){
        // [0,3)
        return MESSAGE[RANDOM.nextInt(3)];
    }
}
