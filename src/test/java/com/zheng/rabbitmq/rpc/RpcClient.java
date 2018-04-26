package com.zheng.rabbitmq.rpc;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zheng.rabbitmq.Constants;

/**
 * @Author zhenglian
 * @Date 2018/4/26 13:40
 */
public class RpcClient {

    private Connection connection;
    private Channel channel;
    
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
    
    
    
    
    
    public static void main(String[] args) throws Exception {
    }
}
