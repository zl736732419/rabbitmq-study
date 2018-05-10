package com.zheng.rabbitmq.queuebottleneck;

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
    private long seqMsg;
    /**
     * 消息主体
     */
    private String msgBody;
    /**
     * 记录消息的deliveryTag,在消费者端消费时填入
     */
    private long deliveryTag;

    public long getSeqMsg() {
        return seqMsg;
    }

    public void setSeqMsg(long seqMsg) {
        this.seqMsg = seqMsg;
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
}
