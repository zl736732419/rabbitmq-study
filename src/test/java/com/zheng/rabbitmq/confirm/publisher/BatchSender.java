package com.zheng.rabbitmq.confirm.publisher;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 生产者采用批量确认的方式实现消息确认
 * @Author zhenglian
 * @Date 2018/5/6 15:37
 */
public class BatchSender {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        factory.setVirtualHost(Constants.V_HOST);
        factory.setUsername(Constants.USER);
        factory.setPassword(Constants.PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(Constants.QUEUE_NAME, false, false, false, null);

        // 消息缓存
        List<String> cacheQueue = new ArrayList<>();
        
        int batch_count = 1;
        int count = 0; // 批次确认，这里因为只发送一条消息，所以设置一批次1个消息
        channel.confirmSelect(); // 进入消息确认模式
        while(true) {
            String message = "Hello World!";
            sendMessage(channel, message);
            cacheQueue.add(message);
            if (++count >= batch_count) {
                try {
                    if (channel.waitForConfirms()) { 
                        // 消息发送成功,清空这一批次的缓存数据
                        cacheQueue.clear();
                    }
                    // 缓存中的消息重新发送
                    for(String cacheMessage : cacheQueue) {
                        sendMessage(channel, cacheMessage);
                    }
                }catch(InterruptedException e) {
                    // 缓存中的消息重新发送
                    for(String cacheMessage : cacheQueue) {
                        sendMessage(channel, cacheMessage);
                    }
                }
            }
            Thread.sleep(1000);
        }
    }

    private static void sendMessage(Channel channel, String message) throws Exception {
        channel.basicPublish("", Constants.QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + message + "'");
    }
}
