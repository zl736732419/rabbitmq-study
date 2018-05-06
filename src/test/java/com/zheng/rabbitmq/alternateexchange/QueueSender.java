package com.zheng.rabbitmq.alternateexchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 备胎exchange直接存储没有被正确路由的消息
 * 而mandatory则是将消息返回给生产者处理
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

        Map<String, Object> params = new HashMap<>();
        params.put("alternate-exchange", "alternateExt");
        channel.exchangeDeclare("normalExt", BuiltinExchangeType.DIRECT, true, false, params);
        // 备胎exchange
        channel.exchangeDeclare("alternateExt", BuiltinExchangeType.FANOUT, true, false, null);

        channel.queueDeclare("normalQueue", true, false, false, null);
        channel.queueDeclare("alternateQueue", true, false, false, null);
        
        channel.queueBind("normalQueue", "normalExt", "normal");
        channel.queueBind("alternateQueue", "alternateExt", "ae");
        
        String message = "Hello World!";
        String queueName = "noExistQueue"; // 交换机无法路由到一个不存在的队列中，所以会将消息转交给备份交换机
        channel.basicPublish("normalExt", queueName, null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
