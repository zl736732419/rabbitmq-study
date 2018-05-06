package com.zheng.rabbitmq.trasaction;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * @Author zhenglian
 * @Date 2018/4/24 16:45
 */
public class QueueSender {
    
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        factory.setVirtualHost(Constants.V_HOST);
        factory.setUsername(Constants.USER);
        factory.setPassword(Constants.PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        String queueName = "transactionQueue";
        channel.queueDeclare(queueName, false, false, false, null);
        String message = "Hello World!";
        // 设置信道进入事务模式
        channel.txSelect();
        try {
            channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
            channel.txCommit();
            System.out.println(" [x] Sent '" + message + "'");
        } catch(Exception e) {
            e.printStackTrace();
            channel.txRollback();
        }

        channel.close();
        connection.close();
    }
}
