package com.mq.rabbitmq.consumer;

import com.mq.rabbitmq.service.impl.HelloWorldServiceImpl;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: Dely
 * @Date: 2019/12/20 12:54
 */
public class Consumer1 {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        HelloWorldServiceImpl service = new HelloWorldServiceImpl();
        //service.receiveForManyWorkers(1);
        service.subscribe();

    }
}
