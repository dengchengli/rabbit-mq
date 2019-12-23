package com.mq.rabbitmq.utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

import java.util.concurrent.TimeoutException;

/**
 * @Author: Dely
 * @Date: 2019/12/20 18:16
 */
public class ChannelUtils {
    private  static Connection connection = null;
    public static Channel commonConnectionChannel() throws IOException, TimeoutException {
        return connection.createChannel();
    }

    static {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setHost("localhost");
            factory.setPort(5672);
            connection  = factory.newConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
