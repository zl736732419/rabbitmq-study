package com.zheng.rabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

public class LogSender {
    // 通过routing key对消息进行分流，不同routing key的消息会被对应相同routing key的queue接受
    private static final String ROUTING_KEY = "kern.critical";
    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 生产者直接与exchange打交道，由exchange与queue进行消息通信
        // 采用topic exchange,可以对routing key采用通配符进行设置
        channel.exchangeDeclare(Constants.EXCHANGE_NAME, "topic");

        String message = "publish subscribe message";

        // 对于sub/pub类型消息，只需设置exchange即可，消费者通过与exchange绑定从而获取exchange中的消息，所以这里的queueName(routingKey)为空
        channel.basicPublish(Constants.EXCHANGE_NAME, ROUTING_KEY, null, 
                message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}