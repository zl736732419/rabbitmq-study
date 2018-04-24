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
 * rabbitmq默认采用的是round-robin消息分发策略，
 * 多个消费者将会分配数量相同的消息
 * 当队列中存在多个消息时，broker将把消息轮询的发送给每一个consumer进行消费
 * 
 * @Author zhenglian
 * @Date 2018/4/24 16:56
 */
public class QueueReceiver {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 队列是否持久化
        boolean durable = true;
        
        channel.queueDeclare(Constants.DURABLE_QUEUE_NAME, durable, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // 设置消费者预取的数量，
        // 这里设置成1意味着broker一次只会发送一个消息给consumer。直到consumer处理完消息并返回消息确认之后，broker才会再发送消息给consumer
        // 这样就杜绝了如果有多个耗时时间不同的任务不会被一直分配给一个consumer进行消费
        // 比如存在5个任务，其中1,3,5任务耗时时间长，2,4任务耗时时间短，存在2个消费者，如果不设置prefetch，那么消费者1将会获取到1,3,5，
        // 消费者2将会获取到2,4，这样导致消费者1一直处于忙阶段，而消费者2可能很快执行完任务而处于等待任务中的情况
        channel.basicQos(1);
        
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
        channel.basicConsume(Constants.DURABLE_QUEUE_NAME, autoAck, consumer);
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }
}
