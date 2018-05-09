package com.zheng.rabbitmq.shovel;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * 这里是为了保证目的queue可以成功发送消息，同时创建出目标exchange和queue，以及他们之间的绑定关系
 * @Author zhenglian
 * @Date 2018/4/24 16:45
 */
public class DestinationSender {
    
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.SHOVEL_DESTINATION);
        factory.setPort(Constants.PORT);
        factory.setVirtualHost(Constants.V_HOST);
        factory.setUsername(Constants.USER);
        factory.setPassword(Constants.PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String exchangeName = "destinationExchange";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, false);

        String queueName = "destinationQueue";
        channel.queueDeclare(queueName, false, false, false, null);

        channel.queueBind(queueName, exchangeName, "");
        
        String message = "Hello World!";
        channel.basicPublish(exchangeName, "", null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
