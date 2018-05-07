package com.zheng.rabbitmq.persistent;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * 持久化消息在rabbitmq服务重启后仍然保存
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

        String exchangeName = "durableExt";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);
        
        String queueName = "durableQueue";
        channel.queueDeclare(queueName, true, false, false, null);
        
        channel.queueBind(queueName, exchangeName, "");
        

        String message = "Hello World!";
        channel.basicPublish(exchangeName, "rk", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
