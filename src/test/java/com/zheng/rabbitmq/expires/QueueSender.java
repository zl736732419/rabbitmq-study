package com.zheng.rabbitmq.expires;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 过期时间分两种：队列过期时间，消息过期时间
 * 如果两者都设置了，那么取最小的值
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

        String message = "Hello World!";

        Map<String, Object> params = new HashMap<>();
        params.put("x-message-ttl", 50000); // 设置消息的过期时间
        // 设置队列的ttl,如果队列在60s内还没有被任何消费者时候或者没有调用basic.get,那么队列会被自动删除
        params.put("x-expires", 60000); 
        
        String queueName = "expireQueue";
        channel.queueDeclare(queueName, false, false, false, params);
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        AMQP.BasicProperties props = builder.expiration("30000")
                .deliveryMode(2) // 持久化消息
                .build();
        channel.basicPublish("", queueName, props, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
