package com.zheng.rabbitmq.shovel;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;

/**
 * 向源队列中发送消息
 * 最后会根据shovel将消息转发到目标exchange中
 * 1. 开启各个节点的shovel/shovel_management插件
 * 2. 在其中一个节点上设置shovel link
 * 这里我选择的是在源节点上设置
 * rabbitmqctl set_parameter shovel hidden_shovel \
 * '{\
 *      "src-uri":"amqp://guest:guest@zl201:5672",\
 *      "src-queue":"sourceQueue",\
 *      "dest-uri":"amqp://guest:guest@zl202:5672",\
 *      "dest-exchange":"destinationExchange",\
 *      "prefetch-count":64,\
 *      "reconnect-delay":5,\
 *      "publish-properties":[],\
 *      "add-forward-headers":true,\
 *      "ack-mode":"on-confirm"\
 *   }'
 *   完成设置后，当向源queue发送消息后，消息会被转发到目标queue中
 * @Author zhenglian
 * @Date 2018/4/24 16:45
 */
public class QueueSender {
    
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.SHOVEL_SOURCE);
        factory.setPort(Constants.PORT);
        factory.setVirtualHost(Constants.V_HOST);
        factory.setUsername(Constants.USER);
        factory.setPassword(Constants.PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queueName = "sourceQueue";
        channel.queueDeclare(queueName, false, false, false, null);
        String message = "Hello World!";
        channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
