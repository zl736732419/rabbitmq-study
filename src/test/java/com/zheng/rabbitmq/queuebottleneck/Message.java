package com.zheng.rabbitmq.queuebottleneck;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * message封装类
 * @Author zhenglian
 * @Date 2018/5/10 22:22
 */
public class Message implements Serializable {
    /**
     * 消息全局有序序号，可以通过它保证消息的有序消费
     */
    private long msgSeq;
    /**
     * 消息主体
     */
    private String msgBody;
    /**
     * 记录消息的deliveryTag,在消费者端消费时填入
     */
    private long deliveryTag;

    public long getMsgSeq() {
        return msgSeq;
    }

    public void setMsgSeq(long msgSeq) {
        this.msgSeq = msgSeq;
    }

    public String getMsgBody() {
        return msgBody;
    }

    public void setMsgBody(String msgBody) {
        this.msgBody = msgBody;
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(long deliveryTag) {
        this.deliveryTag = deliveryTag;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append(msgSeq).append(msgBody).append(deliveryTag).build();
    }
}
