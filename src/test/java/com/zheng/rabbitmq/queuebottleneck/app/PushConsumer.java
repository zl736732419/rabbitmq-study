package com.zheng.rabbitmq.queuebottleneck.app;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.zheng.rabbitmq.queuebottleneck.BottleneckConstants;
import com.zheng.rabbitmq.queuebottleneck.EnumConsumeStatus;
import com.zheng.rabbitmq.queuebottleneck.IMsgCallback;
import com.zheng.rabbitmq.queuebottleneck.Message;
import com.zheng.rabbitmq.queuebottleneck.RmqEncapsulation;

import java.util.Optional;

/**
 * 推模式消费者
 * @Author zhenglian
 * @Date 2018/5/11 8:57
 */
public class PushConsumer {
    private String queue = BottleneckConstants.QUEUE;
    private Integer subdivision = BottleneckConstants.SUBDIVISION; 
    
    private RmqEncapsulation rmqEncapsulation;
    private Connection connection;
    private Channel channel;
    
    public void init() {
        rmqEncapsulation = new RmqEncapsulation(subdivision);
        try {
            connection = rmqEncapsulation.getConnection();
            channel = connection.createChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 消息消费，这里通过注册监听器来实现消息消费
     */
    public void consume() throws Exception {
        rmqEncapsulation.basicConsume(channel, queue, false, "consumer1", new IMsgCallback() {
            @Override
            public EnumConsumeStatus callback(Message message) {
                // 处理消息业务逻辑
                if (!Optional.ofNullable(message).isPresent()) {
                    return EnumConsumeStatus.FAILED;
                }
                System.out.println(message);
                return EnumConsumeStatus.SUCCESS;
            }
        });
    }
    
    public static void main(String[] args) throws Exception {
        PushConsumer consumer = new PushConsumer();
        consumer.init();
        consumer.consume();
    }
}
