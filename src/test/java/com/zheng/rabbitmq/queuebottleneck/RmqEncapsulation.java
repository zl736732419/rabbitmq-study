package com.zheng.rabbitmq.queuebottleneck;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;
import com.zheng.rabbitmq.queuebottleneck.queueindex.QueueIndexLoader;

import java.util.Map;
import java.util.Optional;


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
        int queueIndex = queueIndexLoader.queueIndex(message.getSeqMsg(), subdivisionNum);
        String rk = new StringBuilder(routingKey).append(seperator).append(queueIndex).toString();
        byte[] body = SerializationUtil.serialize(message);
        channel.basicPublish(exchange, rk, mandatory, props, body);
    }
    
}
