package com.zheng.rabbitmq.federation;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * zl51设置联邦连接zl52，zl52作为upstream
 * rabbitmqctl set_parameter federation-upstream my-upstream '{"uri":"amqp://zl52:5672","expires":3600000}'
 * rabbitmqctl set_policy federate-me 'federationQueue' '{"federation-upstream":"my-upstream"}'
 * 如此在zl51,和zl52上都会创建一个联邦队列(federationQueue)
 * 这时通过在zl51上对federationQueue发送消息，可以在zl52节点上通过federationQueue进行消费
 * 如果在zl52设置联邦连接zl51,把zl51作为upstream,这时zl51,zl52相互形成联邦队列，如果在消费消息时不进行消息认证，那么消息将会在这两个节点
 * 上来回传递
 * 联邦link不能对默认的exchange或者rabbitmq内部的exchange建立联邦exchange
 * @Author zhenglian
 * @Date 2018/4/24 16:45
 */
public class QueueSender {
    
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost(Constants.HOST);
        factory.setHost(Constants.UPSTREAM_HOST);
        factory.setPort(Constants.PORT);
        factory.setVirtualHost(Constants.V_HOST);
        factory.setUsername(Constants.USER);
        factory.setPassword(Constants.PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String federationQueue = "federationQueue";
        channel.queueDeclare(federationQueue, false, false, false, null);
        String message = "Hello World!";
        channel.basicPublish("", federationQueue, null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
