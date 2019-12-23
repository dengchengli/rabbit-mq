package com.mq.rabbitmq.service;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: Dely
 * @Date: 2019/12/17 15:51
 */
public interface HelloWorldService {
    /**
     * 单个消费者 使用默认交换机
     * @throws IOException
     * @throws TimeoutException
     */
    void send() throws IOException, TimeoutException;
    void receive() throws IOException, TimeoutException;

    /**
     * 多个消费者的情况 使用默认交换机
     * @throws IOException
     * @throws TimeoutException
     */
    void msgSendManyWorkers() throws IOException, TimeoutException;
    void receiveForManyWorkers(int conId) throws IOException, TimeoutException;

    /**
     * 发布订阅模式：
     */
    void publish() throws IOException, TimeoutException;
    void subscribe() throws IOException, TimeoutException;

    /**
     * 发布确认模式
     */
    void pubAndCom() throws InterruptedException, TimeoutException, IOException;
}
