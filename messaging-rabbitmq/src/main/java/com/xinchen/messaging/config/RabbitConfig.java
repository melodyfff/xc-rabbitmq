package com.xinchen.messaging.config;

import com.xinchen.messaging.pojo.Receiver;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @author Xin Chen (xinchenmelody@gmail.com)
 * @version 1.0
 * @date Created In 2019/7/2 23:03
 */
@Configuration
public class RabbitConfig {
    public static final String topicExchaneName = "xc-exchange";

    public static final String queueName = "xc-queue";

    @Bean
    Queue queue(){
        // 创建新queue, 持久化durable为false
        return new Queue(queueName, false);
    }

    @Bean
    TopicExchange exchange(){
        // 配置exchange, 采用topic主题发布方式
        return new TopicExchange(topicExchaneName);
    }

    @Bean
    Binding binding(Queue queue,TopicExchange exchange){
        // 绑定queue,exchange和routingKey
        // 关于routingKey: '*' 匹配单个单词，'#' 匹配多个单词
        return BindingBuilder.bind(queue).to(exchange).with("hello.#");
    }

    /**
     * 消息监听
     *
     * 将会创建和这个beanName相同的线程池接收处理消息
     *
     * @param connectionFactory ConnectionFactory
     * @param listenerAdapter listenerAdapter
     * @return SimpleMessageListenerContainer
     */
    @Bean("xc-rabbit")
    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory,
                                             MessageListenerAdapter listenerAdapter){
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueName);
        container.setMessageListener(listenerAdapter);
        return container;
    }

    @Bean
    MessageListenerAdapter listenerAdapter(Receiver receiver){

        // 消息监听适配器，通过反射将消息处理委托给目标监听器方法
        // 参数： 委托对象,在接收到消息时的默认处理方法
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }

}
