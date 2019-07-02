package com.xinchen.messaging.runner;

import com.xinchen.messaging.config.RabbitConfig;
import com.xinchen.messaging.pojo.Receiver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 模拟发送和消费消息
 * <p>
 * 仅仅在测试环境下执行
 *
 * @author Xin Chen (xinchenmelody@gmail.com)
 * @version 1.0
 * @date Created In 2019/7/2 23:25
 */
@Component
@Slf4j
@Profile("test")
public class Runner implements CommandLineRunner {

    private final RabbitTemplate rabbitTemplate;
    private final Receiver receiver;

    private static final Random random = new Random();

    public Runner(RabbitTemplate rabbitTemplate, Receiver receiver) {
        this.rabbitTemplate = rabbitTemplate;
        this.receiver = receiver;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Sending message ...");

        for (int i = 0; i < Receiver.N; i++) {

            // 模拟延迟
            Thread.sleep(random.nextInt(5)*1000);

            // exchangeName,routingKey,message
            rabbitTemplate.convertAndSend(RabbitConfig.topicExchaneName, "hello.world", "Hello from RabbitMQ!");
        }

        // 等待pojo介绍到消息后执行countDown()
        receiver.getLatch().await(5, TimeUnit.SECONDS);
    }
}
