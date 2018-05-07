package com.zheng.rabbitmq.exchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * 测试非持久化的exchange
 * 重启后费持久化的exchange会被删除
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
        
        String exchangeName = "transientExt";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, false);
        
        String queueName = "transientQueue";
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queueBind(queueName, exchangeName, "");
        
        
        String message = "Hello World!";
        channel.basicPublish(exchangeName, "hello", null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
