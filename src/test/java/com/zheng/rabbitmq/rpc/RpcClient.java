package com.zheng.rabbitmq.rpc;

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
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * 本示例服务器提供一个斐波那契数的计算服务
 * 客户端通过rpc调用服务器端的计算服务，
 * 并阻塞接受服务器端传递过来的计算结果
 * 通过correlationId关联request与response
 * 通过replyTo queue接受消息响应
 *
 * @Author zhenglian
 * @Date 2018/4/26 13:40
 */
public class RpcClient {

    private Connection connection;
    private Channel channel;

    /**
     * 用于接受服务器的消息响应
     */
    private String replyToQueueName;

    public RpcClient() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);

        connection = factory.newConnection();
        channel = connection.createChannel();

        // 用于接受响应的队列
        replyToQueueName = channel.queueDeclare().getQueue();
    }

    public String call(String message) throws Exception {
        // 标识一个request请求
        final String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .correlationId(corrId)
                .replyTo(replyToQueueName)
                .build();
        channel.basicPublish("", Constants.RPC_QUEUE_NAME, props, message.getBytes(StandardCharsets.UTF_8));

        // 通过阻塞队列达到同步阻塞等待计算结果
        final BlockingQueue<String> resultQueue = new ArrayBlockingQueue<>(1);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String correlationId = properties.getCorrelationId();
                if (Objects.equals(correlationId, corrId)) { // 标识当前服务端响应消息对应客户端请求
                    String result = new String(body, StandardCharsets.UTF_8);
                    resultQueue.offer(result);
                }
            }
        };

        channel.basicConsume(replyToQueueName, consumer);
        // 阻塞直到获取到结果
        return resultQueue.take();
    }


    public void close() {
        try {
            if (Optional.ofNullable(channel).isPresent()) {
                channel.close();
            }
            if (Optional.ofNullable(connection).isPresent()) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        RpcClient client = new RpcClient();
        String result = client.call("5");
        System.out.println("result: " + result);
        client.close();
    }
}
