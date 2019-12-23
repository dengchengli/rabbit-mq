package com.mq.rabbitmq.consumer;

import com.mq.rabbitmq.service.impl.HelloWorldServiceImpl;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: Dely
 * @Date: 2019/12/20 12:45
 */
public class Consumer2 {
    public static void main(String[] args) throws IOException, TimeoutException {
        HelloWorldServiceImpl service = new HelloWorldServiceImpl();
        //service.receiveForManyWorkers(2);
        service.subscribe();
    }
}
