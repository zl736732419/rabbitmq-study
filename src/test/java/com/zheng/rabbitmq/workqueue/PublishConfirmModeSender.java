package com.zheng.rabbitmq.workqueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ReturnListener;
import com.zheng.rabbitmq.Constants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * publish confirm mode保证生产者发送消息到broker的持久化
 * @Author zhenglian
 * @Date 2018/4/25 16:54
 */
public class PublishConfirmModeSender {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 队列是否持久化
        boolean durable = true;
        channel.queueDeclare(Constants.DURABLE_QUEUE_NAME, durable, false, false, null);
//        // 声明持久化交换机，类型为direct
//        channel.exchangeDeclare(Constants.EXCHANGE_NAME, "direct", durable, false, null);
        // 调用confirm.select使channel进入确认模式
        channel.confirmSelect();
        
        // 记录消息投递的deliver tag
        SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<Long>());
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("Ack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
                // 消息成功投递
                if (multiple) {
                    confirmSet.headSet(deliveryTag + 1).clear();
                } else {
                    confirmSet.remove(deliveryTag);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                // 消息没能成功投递，消费者没有进行处理
                System.out.println("Nack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
                if (multiple) {
                    confirmSet.headSet(deliveryTag + 1).clear();
                } else {
                    confirmSet.remove(deliveryTag);
                }
            }
        });
        
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, 
                                     AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("handleReturn");
                System.out.println("replyCode:"+replyCode);
                System.out.println("replyText:"+replyText);
                // 分发失败的消息
                System.out.println("messagebody:"+new String(body));
            }
        });
        
        String message = "durable message";
        // 通过mandatory,处理分发失败的消息
        boolean mandatory = false;
        // 消息持久化
        AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN;
        channel.basicPublish("", Constants.DURABLE_QUEUE_NAME, mandatory, props, 
                message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");

//        channel.close();
//        connection.close();
    }
}
