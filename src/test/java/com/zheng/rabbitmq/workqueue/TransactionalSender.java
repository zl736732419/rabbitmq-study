package com.zheng.rabbitmq.workqueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * @Author zhenglian
 * @Date 2018/4/25 17:41
 */
public class TransactionalSender {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 队列是否持久化
        boolean durable = true;
        channel.queueDeclare(Constants.DURABLE_QUEUE_NAME, durable, false, false, null);

        // 开启事务channel，通过事务保证消息持久化
        channel.txSelect();
        String message = "transactional message";
        // 消息持久化
        AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN;
        channel.basicPublish("", Constants.DURABLE_QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");
        // 提交事务
        channel.txCommit();
        channel.close();
        connection.close();
    }
}
