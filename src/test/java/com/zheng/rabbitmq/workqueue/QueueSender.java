package com.zheng.rabbitmq.workqueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * 要保证rabbitmq的消息持久化，需要做两件事情：
 * 1. 设置队列持久化
 * 2. 设置消息持久化
 * 需要注意的是，单纯的仅仅只是设置消息的持久化属性，并不能完全保证消息真正就持久化了，因为消息在保存到磁盘这个过程还需要短暂的时间
 * rabbitmq默认只是将消息保存到内存缓存中，然后再批量刷进磁盘，所以这个过程中如果rabbitmq挂了，那么消息并没有真正保存到磁盘，
 * 如果需要实现强消息持久化，需要使用生产者消息确认机制
 * 
 * @Author zhenglian
 * @Date 2018/4/24 16:45
 */
public class QueueSender {
    
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 队列是否持久化
        boolean durable = true;
        
        channel.queueDeclare(Constants.DURABLE_QUEUE_NAME, durable, false, false, null);
        
        String message = getMessage(args);
        
        // 消息持久化
        AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN;
        channel.basicPublish("", Constants.DURABLE_QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 1) {
            return "Hello World!";
        }
        return joinStrings(strings, " ");
    }

    private static String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuilder words = new StringBuilder(strings[0]);
        for (int i = 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}
