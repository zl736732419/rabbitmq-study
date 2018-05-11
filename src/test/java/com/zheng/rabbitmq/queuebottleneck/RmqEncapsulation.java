package com.zheng.rabbitmq.queuebottleneck;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.zheng.rabbitmq.Constants;
import com.zheng.rabbitmq.queuebottleneck.queueindex.QueueIndexLoader;
import com.zheng.rabbitmq.queuebottleneck.queueindex.RandomQueueIndexLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * 缓解队列瓶颈
 * rabbitmq方法包装，用于将一个逻辑队列绑定到多个物理队列
 * 在rabbitmq中通过将一个逻辑队列映射成多个队列来提高队列处理能力，可以在消息密集的情况下减轻
 * rabbitmq_amqqueue_process的消费压力
 * 整个过程对客户端透明，客户端将消息发送给一个逻辑的队列名，rabbitmq将其映射成多个队列，
 * 这样客户端发送的消息只会落入其中的一个队列
 * 消费者因此也从原来的一个队列消费转变成从多个队列进行消费
 * @Author zhenglian
 * @Date 2018/5/10 22:26
 */
public class RmqEncapsulation {
    // 队列分片数 表示一个逻辑队列背后的实际队列数
    private int subdivisionNum;
    /**
     * 构建逻辑队列和路由键的分隔符
     */
    private String seperator = "_";
    
    public RmqEncapsulation(int subdivisionNum) {
        this.subdivisionNum = subdivisionNum;
    }
    
    public Connection getConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.PORT);
        factory.setVirtualHost(Constants.V_HOST);
        factory.setUsername(Constants.USER);
        factory.setPassword(Constants.PASSWORD);
        Connection connection = factory.newConnection();
        return connection;
    }
    
    public void closeConnection(Connection connection) throws Exception {
        if (!Optional.ofNullable(connection).isPresent()) {
            return;
        }
        connection.close();
    }

    /**
     * 声明交换器
     * @param channel
     * @param exchange
     * @param type
     * @param durable
     * @param autoDelete
     * @param params
     * @throws Exception
     */
    public void exchangeDeclare(Channel channel, String exchange, String type, boolean durable, 
                                boolean autoDelete, Map<String, Object> params) throws Exception {
        channel.exchangeDeclare(exchange, type, durable, autoDelete, params);
    }

    /**
     * 这里的队列需要传递进来的其实是逻辑队列名
     * 这里需要将逻辑队列名映射为subdivision个物理队列
     * @param channel
     * @param queue 逻辑队列名，映射subdivision个物理队列
     * @param durable
     * @param exclusive
     * @param autoDelete
     * @param params
     * @throws Exception
     */
    public void queueDeclare(Channel channel, String queue, boolean durable, boolean exclusive, boolean autoDelete, 
                             Map<String, Object> params) throws Exception {
        String queueName;
        for(int i = 0; i < subdivisionNum; i++) {
            queueName = new StringBuilder(queue).append(seperator).append(i).toString();
            channel.queueDeclare(queueName, durable, exclusive, autoDelete, params);
        }
    }

    /**
     * 将逻辑队列与交换器进行绑定
     * 其实是映射到了各个物理队列与exchange绑定
     * @param channel
     * @param queue 逻辑队列名
     * @param exchange
     * @param routingKey 逻辑路由键，需要映射到每一个物理队列与交换器的绑定
     * @param params
     * @throws Exception
     */
    public void queueBind(Channel channel, String queue, String exchange, String routingKey, 
                                 Map<String, Object> params) throws Exception {
        String rkName;
        String queueName;
        for(int i = 0; i < subdivisionNum; i++) {
            queueName = new StringBuilder(queue).append(seperator).append(i).toString();
            rkName = new StringBuilder(routingKey).append(seperator).append(i).toString();
            channel.queueBind(queueName, exchange, rkName, params);
        }
    }

    /**
     * 发布一个消息
     * @param channel
     * @param exchange
     * @param routingKey
     * @param mandatory
     * @param props
     * @param message
     * @throws Exception
     */
    public void basicPublish(Channel channel, String exchange, String routingKey, boolean mandatory,
                             AMQP.BasicProperties props, Message message, QueueIndexLoader queueIndexLoader) throws Exception {
        int queueIndex = queueIndexLoader.queueIndex(message.getMsgSeq(), subdivisionNum);
        String rk = new StringBuilder(routingKey).append(seperator).append(queueIndex).toString();
        byte[] body = SerializationUtil.serialize(message);
        channel.basicPublish(exchange, rk, mandatory, props, body);
    }

    /**
     * 拉模式消费消息
     * 先从一个随机的队列中获取消息，如果没有取到消息再遍历队列消费消息
     * 这样可以避免每次获取消息都需要顺序遍历队列，导致前面的队列消息一直被消费，后面队列的消息会被长久积压
     * 这里实现的是消息的自动确认，当然也可以实现为手动确认方式，这里没有做实现
     * 推模式实现消息的手动确认
     * @param channel
     * @param queue
     * @param autoAck
     * @throws Exception
     */
    public Message basicGet(Channel channel, String queue, boolean autoAck) throws Exception {
        QueueIndexLoader loader = new RandomQueueIndexLoader();
        int queueIndex = loader.queueIndex(0, subdivisionNum);
        String queueName = new StringBuilder(queue).append(seperator).append(queueIndex).toString();
        GetResponse getResponse = channel.basicGet(queueName, autoAck);
        if (Optional.ofNullable(getResponse).isPresent()) {
            return parseMessage(getResponse);
        }
        
        for (int i = 0; i < subdivisionNum; i++) {
            if (Objects.equals(i, queueIndex)) {
                continue;
            }
            queueName = new StringBuilder(queue).append(seperator).append(i).toString();
            getResponse = channel.basicGet(queueName, autoAck);
            if (Optional.ofNullable(getResponse).isPresent()) {
                return parseMessage(getResponse);
            }
        }
        return null;
    }

    /**
     * 异步监听消费,这里是实现消息的有序消费，需要生产者采用RoundRobinQueueIndexLoader
     * @param channel
     * @param queue
     * @param autoAck
     * @param consumerTag
     * @param callback
     * @throws Exception
     */
    public void basicConsume(Channel channel, String queue, boolean autoAck, String consumerTag, IMsgCallback callback) throws Exception {
        Map<Integer, BlockingQueue<Message>> queueMap = new HashMap<>();
        // 将rmq消息存入缓存
        startConsume(channel, queue, autoAck, consumerTag, queueMap);
        int i = 0;
        BlockingQueue<Message> cache;
        Message message;
        int index;
        while (true) {
            index = i % subdivisionNum;
            cache = queueMap.get(index);
            // 取出的时候就已经从队列中消费出来了
            message = cache.peek();
            if (Optional.ofNullable(message).isPresent()) {
                EnumConsumeStatus consumeStatus = callback.callback(message);
                cache.remove(message);
                if (Objects.equals(consumeStatus, EnumConsumeStatus.SUCCESS)) {
                    // 如果非自动确认，需要进行手动确认
                    if (!autoAck) {
                        channel.basicAck(message.getDeliveryTag(), false);
                    }
                } else {
                    if (!autoAck) {
                        // 如果重新入队很可能会打破消息的顺序，所以这里不能让其入队，可以在生产者端增加publisher confirm机制处理该类消息
                        channel.basicReject(message.getDeliveryTag(), false);
                    }
                }
                i++;
                // 这里考虑如果无限增大i,最终会出现溢出问题
                if (Objects.equals(i, subdivisionNum)) {
                    i = 0;
                }
            } else {
                // 如果队列为空，则表示整个逻辑队列为空或者消息已经被消费完了，所以需要等待一段时间在处理
                System.out.println("current queue is empty!");
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }
        
    }
    
    /**
     * 异步监听器模式将rmq中的消息存入缓存队列
     * 这里
     * @param channel
     * @param queue
     * @throws Exception
     */
    public void startConsume(Channel channel, String queue, boolean autoAck, String consumerTag, 
                             Map<Integer, BlockingQueue<Message>> queueMap) throws Exception {
        // 将各个队列的消息分别存放到不同的缓冲队列中，这里主要是为了实现客户端顺序消费
        String queueName;
        String realConsumerTag;
        BlockingQueue<Message> cache;
        for(int i = 0; i < subdivisionNum; i++) {
            cache = queueMap.get(i);
            if (!Optional.ofNullable(cache).isPresent()) {
                cache = new ArrayBlockingQueue<>(100);
                queueMap.put(i, cache);
            }
            queueName = new StringBuilder(queue).append(seperator).append(i).toString();
            // 消费者标识需要唯一
            realConsumerTag = new StringBuilder(consumerTag).append(seperator).append(i).toString();
            channel.basicConsume(queueName, autoAck, realConsumerTag, new NewConsumer(channel, cache));
        }
    }

    /**
     * 注册监听异步消费，将队列中的消息缓存到指定的缓冲队列中
     */
    private static class NewConsumer extends DefaultConsumer {
        private BlockingQueue<Message> cache;
        
        public NewConsumer(Channel channel, BlockingQueue<Message> cache) {
            super(channel);
            this.cache = cache;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, 
                                   byte[] body) throws IOException {
            long deliveryTag = envelope.getDeliveryTag();
            Message message = SerializationUtil.deserialize(body, Message.class);
            if (!Optional.ofNullable(message).isPresent()) {
                return;
            }
            message.setDeliveryTag(deliveryTag);
            cache.offer(message);
        }
    }

    /**
     * 解析消息
     * @param getResponse
     * @return
     */
    private Message parseMessage(GetResponse getResponse) {
        if (!Optional.ofNullable(getResponse).isPresent()) {
            return null;
        }
        byte[] body = getResponse.getBody();
        Message message = SerializationUtil.deserialize(body, Message.class);
        message.setDeliveryTag(getResponse.getEnvelope().getDeliveryTag());
        return message;
    }

}
