package com.xinchen.messaging.pojo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/**
 * @author Xin Chen (xinchenmelody@gmail.com)
 * @version 1.0
 * @date Created In 2019/7/2 22:59
 */
@Component
@Slf4j
public class Receiver {

    /** 模拟接收多少条 */
    public static final int N = 10;

    /** 发出信号表示已经收到消息 */
    private CountDownLatch latch = new CountDownLatch(N);


    public void receiveMessage(String message){
        log.info("Received <{}>", message);
        latch.countDown();
    }

    public CountDownLatch getLatch(){
        return latch;
    }
}
