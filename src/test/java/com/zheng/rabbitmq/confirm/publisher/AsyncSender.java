package com.zheng.rabbitmq.confirm.publisher;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

/**
 * publish confirm mode保证生产者发送消息到broker的持久化
 * 生产者采用异步监听的方式实现消息确认
 * @Author zhenglian
 * @Date 2018/4/25 16:54
 */
public class AsyncSender {
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
        // 消息缓存记录deliver tag -> message
        Map<Long, String> cache = new HashMap<>();
        
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("Ack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
                // 消息成功投递
                if (multiple) {
                    SortedSet<Long> okDeliverTags = confirmSet.headSet(deliveryTag + 1);
                    // 清除缓存
                    for (Long key : okDeliverTags) {
                        cache.remove(key);
                    }
                    
                    okDeliverTags.clear();
                } else {
                    // 清除缓存
                    cache.remove(deliveryTag);
                    confirmSet.remove(deliveryTag);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                // 消息没能成功投递，消费者没有进行处理
                System.out.println("Nack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
                if (multiple) {
                    SortedSet<Long> needReDeliveryTags = confirmSet.headSet(deliveryTag + 1);
                    String message;
                    for (Long key : needReDeliveryTags) {
                        message = cache.get(key);
                        sendMessage(channel, message, confirmSet, cache);
                    }
                    needReDeliveryTags.clear();
                } else {
                    String message = cache.get(deliveryTag);
                    sendMessage(channel, message, confirmSet, cache);
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
        sendMessage(channel, message, confirmSet, cache);
    }

    private static void sendMessage(Channel channel, String message, 
                                    SortedSet<Long> confirmSet, 
                                    Map<Long, String> cache){
        if (!Optional.ofNullable(message).isPresent()) {
            return;   
        }
        long nextSeqNo = channel.getNextPublishSeqNo();
        // 消息持久化
        AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN;
        try {
            channel.basicPublish("", Constants.DURABLE_QUEUE_NAME, false, props,
                    message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            try {
                channel.close();
                channel.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            } catch (TimeoutException e1) {
                e1.printStackTrace();
            }
        }
        System.out.println(" [x] Sent '" + message + "'");
        confirmSet.add(nextSeqNo);
        cache.put(nextSeqNo, message);
    }
}
