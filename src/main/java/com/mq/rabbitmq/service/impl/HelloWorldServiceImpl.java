package com.mq.rabbitmq.service.impl;

import com.mq.rabbitmq.service.HelloWorldService;
import com.mq.rabbitmq.utils.ChannelUtils;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

/**
 * @Author: Dely
 * @Date: 2019/12/17 15:52
 */

/**
 * Hello World 篇:基本接口的调用
 * pro ---> queque ---> con
 */
public class HelloWorldServiceImpl implements HelloWorldService {
    private final static String host = "localhost";
    private final static int port = 5672;
    private final static String queueName = "hello";
    private final static String manyQueue = "many_workers";
    private final static String PUB_SUB_QUEUE = "pub_sub_queue";
    private final static String PUB_SCRIBE_EXCHANGE = "pub_scr";
    private final static String temp_queue = "tmp";

    private final static int MESSAGE_COUNT = 50_000;

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        HelloWorldServiceImpl service = new HelloWorldServiceImpl();
        //service.send();
        //service.receive();

        //service.msgSendManyWorkers();

        //service.publish();

        service.pubAndCom();

    }

    @Override
    public void send() throws IOException, TimeoutException {
        /**
         *  创建连接工厂
         */
        ConnectionFactory factory = new ConnectionFactory();
        //设置连接的代理地址
        factory.setHost(host);
        factory.setPort(port);

        /**
         *  通过工厂创建连接
         */
        try (Connection connection = factory.newConnection();
             /**
              *  通过连接创建Channel
              */
             Channel channel = connection.createChannel()) {


            channel.queueDeclare(queueName, false, false, false, null);
            String message = "Hello World";

            channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));

            System.out.println("发送成功");
        }

    }


    @Override
    public void receive() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        factory.setPort(port);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });

    }


    @Override
    public void msgSendManyWorkers() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);

        System.out.println("生产者已启动---------");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(manyQueue, true, false, false, null);
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String msg = scanner.next();
                channel.basicPublish("", manyQueue, MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes("utf-8"));
                System.out.println("消息发送成功：" + msg);
            }
        }
    }

    @Override
    public void receiveForManyWorkers(int conId) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(manyQueue, true, false, false, null);
        System.out.println("worker" + conId + "等待接收消息：--------");

        //manyNormal(channel,conId);
        manyQos(channel, conId);

    }

    private static void manyNormal(Channel channel, int conId) throws IOException {
        /**
         * 回调
         */
        DeliverCallback callback = ((consumerTag, message) -> {
            /**
             * 从message里边获取消息体
             */
            String msg = new String(message.getBody(), "utf-8");

            /**
             * 处理消息
             */
            doWork(msg, conId);
        });

        /**
         * 注册消费者
         */
        channel.basicConsume(manyQueue, false, callback, consumerTag -> {
        });
    }

    /**
     * 该类型是如果自动开启自动确认，则消息会均匀分配给消费者，如果没开启，则需要手动回调确认。需要消息和队列都持久化才能够使消息能够在重启时仍然保留，但是由于
     * rabbitmq可能存在崩溃的状态，此时因为rabbitmq不是直接同步持久化消息到磁盘，所以可能存在没有存储到磁盘的消息，故而不能完全保证消息的不丢失。
     * 如果需要保证完全不丢失，要使用确认发送确认的方式。
     *
     * @param channel
     * @param id
     * @throws IOException
     */
    private static void manyQos(Channel channel, int id) throws IOException {
        /**
         * 一条消息处理完再推送另外一条
         */
        channel.basicQos(1);
        DeliverCallback callback = (consumerTag, delivery) -> {
            /**
             *获取消息体
             */
            String msg = new String(delivery.getBody(), "utf-8");

            /**
             * 执行消息
             */
            doWork(msg, id);

            /**
             * 消息确认
             */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

        };

        channel.basicConsume(manyQueue, false, callback, consumerTag -> {
        });

    }

    /**
     * 这是发布订阅中的生产者
     */
    @Override
    public void publish() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(port);
        factory.setHost(host);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            /**
             * 定义交换机
             */
            channel.exchangeDeclare(PUB_SCRIBE_EXCHANGE, BuiltinExchangeType.FANOUT.getType());
            Scanner scanner = new Scanner(System.in);
            System.out.println("已准备生产-----------------");
            while (scanner.hasNext()) {
                String msg = scanner.next();
                channel.basicPublish(PUB_SCRIBE_EXCHANGE, "", null, msg.getBytes("utf-8"));
            }
        }
    }

    /**
     * 这是发布订阅中的订阅者
     */
    @Override
    public void subscribe() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        final Connection connection = factory.newConnection();
        //final Channel channel = connection.createChannel();
        final Channel channel = ChannelUtils.commonConnectionChannel();

        /**
         * 定义交换机
         */
        channel.exchangeDeclare(PUB_SCRIBE_EXCHANGE, BuiltinExchangeType.FANOUT.getType());
        /**
         * 绑定队列到交换机
         */
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare();
        //这里自动生成的队列必须是一个channel独占的，如果是共享的话就不符合发布订阅模式，因为共享的队列里边的数据会被多个订阅者轮流使用的。
        String queue = declareOk.getQueue();
        channel.queueBind(queue, PUB_SCRIBE_EXCHANGE, "");

        /**
         * 消息回调
         */
        DeliverCallback callback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), "utf-8");
            System.out.println(msg);
        };

        /**
         * 注册消费者
         */
        channel.basicConsume(queue, true, callback, consumerTag -> {
        });
    }

    @Override
    public void pubAndCom() throws InterruptedException, TimeoutException, IOException {
        //batchPublish();
        asynPublish();
    }

    /**
     * 逐条同步确认:
     * 1.设置channel为确认channel。
     * 2.发布消息。
     * 3.等待确认
     */
    private static void onePublish() throws IOException, TimeoutException, InterruptedException {
        Channel channel = ChannelUtils.commonConnectionChannel();
        /**
         * 定义交换机
         */
        channel.exchangeDeclare(PUB_SCRIBE_EXCHANGE, BuiltinExchangeType.FANOUT);

        /**
         * 将channel设置为确认channel
         */
        channel.confirmSelect();
        for (int i = 1; i < MESSAGE_COUNT; i++) {
            String msg = String.valueOf(i);
            /**
             * 发送消息
             */
            channel.basicPublish(PUB_SCRIBE_EXCHANGE, "", null, msg.getBytes("utf-8"));
            /**
             * 同步等待确认
             */
            System.out.format("已经发送消息%,d", i);
            System.out.println();
            channel.waitForConfirmsOrDie(0);
        }
        System.out.println("消息全部发送完成");

    }

    /**
     * 批量同步确认：
     * 1.设置channel为确认channel
     * 2.如果发送数量为批量的限定，则等待上衣个消息发送回来的确认。并且重置计数为0
     * 3.正常发送消息，并且计数加1。
     * 4.循环的最后要判断一下计数是否为0，如果不为，证明发送的所有数据不是批量的整数倍，故而最后的数据需要再确认一下。
     */
    private static void batchPublish() throws IOException, TimeoutException, InterruptedException {
        Channel channel = ChannelUtils.commonConnectionChannel();
        channel.exchangeDeclare(PUB_SCRIBE_EXCHANGE, BuiltinExchangeType.FANOUT);
        channel.confirmSelect();

        int count = 0;
        int batchSize = 100;

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String msg = String.valueOf(i);

            if (count == batchSize) {
                /**
                 * 一次确认一批数据
                 */
                channel.waitForConfirmsOrDie(5_000);
                count = 0;
                System.out.println("批量确认" + i + "个消息");
            }
            channel.basicPublish(PUB_SCRIBE_EXCHANGE, "", null, msg.getBytes("utf-8"));
            System.out.println("消息" + msg + "已发送");
            count++;
        }
        if (count > 0) {
            channel.waitForConfirmsOrDie(5_000);
        }
        System.out.println("已经批量发送完");
    }

    /**
     * 异步回调确认:
     *   1.设置通道为可确认通道
     *   2.设置回调，并且是批量异步确认。
     *   3.设置确认失败回调。
     *   4.注册回调监听
     *   5.发布消息。
     */
    private static void asynPublish() throws IOException, TimeoutException {
        Channel channel = ChannelUtils.commonConnectionChannel();
        channel.exchangeDeclare(PUB_SCRIBE_EXCHANGE, BuiltinExchangeType.FANOUT);

        channel.confirmSelect();

        ConcurrentNavigableMap<Long, String> map = new ConcurrentSkipListMap<>();

        Map<String, Integer> size = new ConcurrentHashMap<>(1);
        size.put("size",0);

        /**
         * 回调处理
         */

        ConfirmCallback confirmCallback = (sequenceNumber, multiple) -> {
            if (multiple) {
                /**
                 * 确认该序号及其之前的所有序号  回调的序号是无序的。
                 */
                ConcurrentNavigableMap<Long, String> subMap = map.headMap(sequenceNumber, true);
                System.out.println("确认的大小为"+ subMap.size());
                int count = size.get("size")+1;
                size.put("size", count);
                subMap.clear();
            } else {
                /**
                 * 不是批量的话，就直接一个一个处理。
                 */
                map.remove(sequenceNumber);
            }

        };

        /**
         * rabbitmq 服务器处理失败的回调
         */
        ConfirmCallback nackCallBack = (sequenceNumber, multiple) -> {
            /**
             * 获取数据
             */
            String msg = map.get(sequenceNumber);
            System.out.println(msg + "消息确认失败");
            /**
             * 通过调用这个方法来清除没有被确认的消息，目的是使得都能获取到争取的或者错误的反馈的事件，demo是这样而已，因为他要计算发送的用时，所以需要清除，
             * 真正的业务处理根据具体情况。
             */
            confirmCallback.handle(sequenceNumber, multiple);

        };

        /**
         * 注册回调监听
         */
        channel.addConfirmListener(confirmCallback, nackCallBack);

        /**
         * 发送消息
         */
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String msg = String.valueOf(i);
            long sequence = channel.getNextPublishSeqNo();
            map.put(sequence,msg);
            channel.basicPublish(PUB_SCRIBE_EXCHANGE,"",null,msg.getBytes("utf-8"));
            System.out.println(String.format("序号为：%s的消息--%s已发送", sequence, msg));
        }
        System.out.println("消息发送成功");
        System.out.println(size.get("size"));
        channel.close();

    }


    private static void doWork(String msg, int id) {
        System.out.println("消费者" + id + "已经接收到消息：" + msg);
        try {
            Thread.sleep(id * 1000);
            System.out.println("消费者" + id + "消息处理结束:" + msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
