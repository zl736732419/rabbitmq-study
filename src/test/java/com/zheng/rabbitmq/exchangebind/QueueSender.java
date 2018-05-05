package com.zheng.rabbitmq.exchangebind;

import com.rabbitmq.client.BuiltinExchangeType;
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
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("source", BuiltinExchangeType.DIRECT, true, false, null);
        channel.exchangeDeclare("destination", BuiltinExchangeType.DIRECT, true, false, null);
        // 将source中routingkey=ext的消息转发到destination
        channel.exchangeBind("destination", "source", "ext");
        
        channel.queueDeclare("destinationQueue", false, false, false, null);
        channel.queueDeclare("sourceQueue", false, false, false, null);
        
        channel.queueBind("destinationQueue", "destination", "ext");
        channel.queueBind("sourceQueue", "source", "ext");
        
        String message = "Hello World!";
        channel.basicPublish("source", "ext", null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
