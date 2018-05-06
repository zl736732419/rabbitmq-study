package com.zheng.rabbitmq.expires;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 死信队列
 * 1. 消息被拒绝并无法重新入队
 * 2. 消息过期
 * 3. 队列达到最大长度
 * 以上三种情况，消息会进入死信队列
 * @Author zhenglian
 * @Date 2018/4/24 16:45
 */
public class DLXQueueSender {
    
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        factory.setVirtualHost(Constants.V_HOST);
        factory.setUsername(Constants.USER);
        factory.setPassword(Constants.PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 普通exchange
        channel.exchangeDeclare("normalExt", BuiltinExchangeType.FANOUT, true, false, null);
        // 死信exchange
        channel.exchangeDeclare("dlxExt", BuiltinExchangeType.DIRECT, true, false, null);

        Map<String, Object> params = new HashMap<>();
        params.put("x-message-ttl", 20000); // 设置消息的过期时间
        params.put("x-dead-letter-exchange", "dlxExt");
        params.put("x-dead-letter-routing-key", "dlx");
        // 普通队列
        channel.queueDeclare("normalQueue", true, false, false, params);
        // 死信队列
        channel.queueDeclare("dlxQueue", true, false, false, null);

        channel.queueBind("normalQueue", "normalExt", "");
        channel.queueBind("dlxQueue", "dlxExt", "dlx");
        
        String message = "Hello World!";
        channel.basicPublish("normalExt", "whatever", null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
