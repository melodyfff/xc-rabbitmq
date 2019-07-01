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
 * 如果消费者死亡（其通道关闭，连接关闭或TCP连接丢失）而不发送确认，RabbitMQ将理解消息未完全处理并将重新排队。
 * 如果同时有其他在线消费者，则会迅速将其重新发送给其他消费者。
 * 这样你就可以确保没有消息丢失，即使工人偶尔会死亡。
 *
 *
 *
 * 在接受到消息并处理途中断开通道，该消息将重回队列中，发送给其他消费者。
 *
 *
 * @author xinchen
 * @version 1.0
 * @date 01/07/2019 12:55
 */
@Slf4j
public class WorkerLostMessage {
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

        // 一次只接受一个未包装的消息
        channel.basicQos(1);

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
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
            }
        };

        // 消费队列
        // 队列名称,是否autoAck,传递消息时的回调,取消消费时的回调
        // 必须设置为false才会生效
        boolean autoAck = false;

        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }


    private static void doWork(String task) throws InterruptedException {
        log.info(" [-] Handle message '{}'",task);
        Thread.sleep((random.nextInt(5))*10000);
    }

}
