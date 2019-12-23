package com.mq.rabbitmq.consumer;

import com.mq.rabbitmq.service.impl.HelloWorldServiceImpl;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: Dely
 * @Date: 2019/12/20 12:46
 */
public class Consumer3 {
    public static void main(String[] args) throws IOException, TimeoutException {
        HelloWorldServiceImpl service = new HelloWorldServiceImpl();
        //service.receiveForManyWorkers(3);

        service.subscribe();
    }
}
