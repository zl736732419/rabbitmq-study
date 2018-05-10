package com.zheng.rabbitmq.queuebottleneck.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.zheng.rabbitmq.queuebottleneck.Message;
import com.zheng.rabbitmq.queuebottleneck.RmqEncapsulation;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 拉模式消费消息
 * @Author zhenglian
 * @Date 2018/5/10 23:35
 */
public class PullConsumer {
    private String queue = "queue";

    private RmqEncapsulation rmq;
    private Channel channel;
    private Connection connection;
    
    public void init() {
        rmq = new RmqEncapsulation(4);
        try {
            connection = rmq.getConnection();
            channel = connection.createChannel();
            rmq.queueDeclare(channel, queue, true, false, false, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void consume() throws Exception {
        while (true) {
            Message message = rmq.basicGet(channel, queue, true);
            if (Optional.ofNullable(message).isPresent()) {
                System.out.println(message);
            } else {
                TimeUnit.MILLISECONDS.sleep(1000);
                System.out.println("current queue is empty waiting 1s for enqueuing message to consume!!!");
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        PullConsumer consumer = new PullConsumer();
        consumer.init();
        consumer.consume();
    }
}
