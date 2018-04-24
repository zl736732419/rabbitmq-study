package com.zheng.rabbitmq.workqueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.zheng.rabbitmq.Constants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Author Administrator
 * @Date 2018/4/24 16:56
 */
public class QueueReceiver {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("zl52");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 队列是否持久化
        boolean durable = true;
        
        channel.queueDeclare(Constants.QUEUE_NAME, durable, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");


                try {
                    doWork(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println(" [x] Done");
                    // 当消息消费完成后消费者手动确认该消息已经被消费，这时broker可以安全的删除队列中的该消息
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        // 消息自动确认，使用自动确认后当消费者接收到消息后就会立马对该消息进行确认，broker就会可以删除当前消息，
        // 但是如果消费者在处理消息的过程中挂掉了，也就是说这个消息还没有被消费完成，那么这个消息也就丢失了，无法再被其他消费者消费
//        boolean autoAck = true;
        // 消费者手动确认消息
        boolean autoAck = false;
        channel.basicConsume(Constants.QUEUE_NAME, autoAck, consumer);
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }
}
