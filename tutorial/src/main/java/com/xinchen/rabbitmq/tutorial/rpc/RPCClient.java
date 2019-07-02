package com.xinchen.rabbitmq.tutorial.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.xinchen.rabbitmq.tutorial.CommonConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;


/**
 *
 *
 * Remote procedure call (RPC)
 *
 * reference: https://www.rabbitmq.com/tutorials/tutorial-six-java.html
 *
 * @author xinchen
 * @version 1.0
 * @date 02/07/2019 16:11
 */
@Slf4j
public class RPCClient implements AutoCloseable{

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        connection = CommonConnectionFactory.FACTORY.newConnection();
        channel = connection.createChannel();
    }


    public static void main(String[] args) {
        try (RPCClient fibonacciRpc = new RPCClient()){
          for (int i = 0;i<32;i++){
              String str = Integer.toString(i);
              log.info(" [x] Requesting fib({})",str);
              String response = fibonacciRpc.call(str);
              log.info(" [.] Got '{}",response);
          }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();
        String replyQueueName = channel.queueDeclare().getQueue();

        AMQP.BasicProperties props = new AMQP.BasicProperties()
                .builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        // 交换器名称,路由键,支持消息的其他属性-路由标题等,消息体
        channel.basicPublish("",requestQueueName,props,message.getBytes(StandardCharsets.UTF_8));
        log.info(" [x] Sent '{}'",message);

        // 阻塞队列
        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        // 队列名称,是否autoAck,传递消息时的回调,取消消费时的回调
        // 返回由服务器生成的consumerTag
        String cTag = channel.basicConsume(replyQueueName, true, ((consumerTag, delivery) -> {
            // 判断CorrelationId
            if (delivery.getProperties().getCorrelationId().equals(corrId)){
                response.offer(new String(delivery.getBody(), StandardCharsets.UTF_8));
            }
        }),consumerTag -> {});


        // 从队列中获取结果
        String result = response.take();
        // 取消消费者
        channel.basicCancel(cTag);
        return result;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
