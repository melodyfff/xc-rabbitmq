package com.xinchen.rabbitmq.tutorial;

import com.rabbitmq.client.ConnectionFactory;


/**
 * 公共连接工厂获取连接
 *
 * @author Xin Chen (xinchenmelody@gmail.com)
 * @version 1.0
 * @date Created In 2019/7/2 0:08
 */
public final class CommonConnectionFactory {
    public static final ConnectionFactory FACTORY = new ConnectionFactory();

    static {
        FACTORY.setHost("localhost");
        // default port 5672
        FACTORY.setPort(5672);
        // default user guest/guest
        FACTORY.setUsername("guest");
        FACTORY.setPassword("guest");
    }
}
