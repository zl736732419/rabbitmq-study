package com.zheng.rabbitmq.queuebottleneck.app;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import com.zheng.rabbitmq.queuebottleneck.Message;
import com.zheng.rabbitmq.queuebottleneck.RmqEncapsulation;
import com.zheng.rabbitmq.queuebottleneck.queueindex.QueueIndexLoader;
import com.zheng.rabbitmq.queuebottleneck.queueindex.RandomQueueIndexLoader;

/**
 * 生产者，发送负责发送消息到逻辑队列中
 * @Author zhenglian
 * @Date 2018/5/10 22:47
 */
public class Producer {
    private String exchange = "exchange";
    private String queue = "queue";
    private String rk = "rk";

    private RmqEncapsulation rmq;
    private Channel channel;
    private Connection connection;
    private QueueIndexLoader queueIndexLoader;
    
    public Producer(QueueIndexLoader queueIndexLoader) {
        this.queueIndexLoader = queueIndexLoader;
    }
    
    
    public void init() {
        rmq = new RmqEncapsulation(4);
        try {
            connection = rmq.getConnection();
            channel = connection.createChannel();
            rmq.exchangeDeclare(channel, exchange, BuiltinExchangeType.DIRECT.getType(), true, false, null);
            rmq.queueDeclare(channel, queue, true, false, false, null);
            rmq.queueBind(channel, queue, exchange, rk, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void send() {
        Message message;
        for(int i = 0; i < 100; i++) {
            message = new Message();
            message.setMsgSeq(i);
            message.setMsgBody("rabbitmq encapsulation : " + i);
            try {
                rmq.basicPublish(channel, exchange, rk, false, MessageProperties.PERSISTENT_TEXT_PLAIN, message, queueIndexLoader);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("消息发送完毕!");
    }

    public static void main(String[] args) throws Exception {
        QueueIndexLoader loader = new RandomQueueIndexLoader();
        Producer producer = new Producer(loader);
        producer.init();
        producer.send();
        producer.rmq.closeConnection(producer.connection);
    }
}
