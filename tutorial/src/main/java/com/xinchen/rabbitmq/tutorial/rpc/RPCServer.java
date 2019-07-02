package com.xinchen.rabbitmq.tutorial.rpc;

import com.rabbitmq.client.AMQP;
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
 * Remote procedure call (RPC)
 *
 * reference: https://www.rabbitmq.com/tutorials/tutorial-six-java.html
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 15:14
 */
@Slf4j
public class RPCServer {
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    /**
     * 模拟业务处理
     * 返回斐波那契数
     * @param n 第几位
     * @return int
     */
    private static int fib(int n){
        if(0 == n){
            return 0;
        }
        if (1==n){
            return 1;
        }
        return fib(n - 1) + fib(n - 2);
    }


    public static void main(String[] args) {
        final ConnectionFactory factory = CommonConnectionFactory.FACTORY;
        try (Connection connection = factory.newConnection()){

            Channel channel = connection.createChannel();
            // 队列名,队列是否持久化,队列是否独占(仅次此连接),队列是否自动删除,构造参数
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            // 清除队列内容
            channel.queuePurge(RPC_QUEUE_NAME);

            // 为了在多个服务器上平均分配负载,设置prefetchCount=1,一次只接受1个
            channel.basicQos(1);

            // 同步锁对象
            Object monitor = new Object();


            // 客户端发送带有两个属性的消息: replyTo
            // replyTo : 设置为仅为请求创建的匿名独占队列
            // correlationId : 设置为每个请求的唯一值
            DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
                // 应答返回参数
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties()
                        .builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();

                String response = "";

                try {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    int n = Integer.parseInt(message);
                    log.info(" [.] fib({})",message);
                    response += fib(n);
                } catch (RuntimeException e){
                    log.error(" [.] {}",e.toString());
                } finally {
                    // 交换器名称,路由键,支持消息的其他属性-路由标题等,消息体
                    channel.basicPublish("",delivery.getProperties().getReplyTo(),replyProps,response.getBytes(StandardCharsets.UTF_8));
                    // 交付标记,是否接收所有acknowledge deliveryTag
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);

                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor){
                        monitor.notify();
                    }

                }

            });

            log.info(" [*] Waiting for messages. To exit press CTRL+C ");
            // 队列名称,是否autoAck,传递消息时的回调,取消消费时的回调
            channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {}));

            while (true){
                synchronized (monitor){
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }
}
