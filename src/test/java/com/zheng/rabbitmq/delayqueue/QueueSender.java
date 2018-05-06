package com.zheng.rabbitmq.delayqueue;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * rabbitmq没有定义延迟队列的类型，但是可以通过TTL和DLX模拟延迟队列的效果
 * 这里实现的是延迟10s
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
        
        String extName = "ext.normal.10s";
        String dlxExtName = "ext.dlx.10s";
        channel.exchangeDeclare(extName, BuiltinExchangeType.FANOUT, true);
        channel.exchangeDeclare(dlxExtName, BuiltinExchangeType.DIRECT, true);
        
        Map<String, Object> params = new HashMap<>();
        params.put("x-message-ttl", 10000);
        params.put("x-dead-letter-exchange", "ext.dlx.10s");
        params.put("x-dead-letter-routing-key", "dlx.10s");
        String queueName = "queue.normal.10s";
        String dlxQueueName = "queue.dlx.10s";
        channel.queueDeclare(queueName, true, false, false, params);
        channel.queueDeclare(dlxQueueName, true, false, false, null);
        
        channel.queueBind(queueName, extName, "");
        channel.queueBind(dlxQueueName, dlxExtName, "dlx.10s");

        String message = "Hello World!";
        channel.basicPublish(extName, queueName, null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
