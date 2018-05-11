package com.zheng.rabbitmq.queuebottleneck;

/**
 * @Author zhenglian
 * @Date 2018/5/11 11:53
 */
public interface IMsgCallback {
    /**
     * 交由客户端实现具体消息的业务逻辑
     * @param message
     * @return
     */
    EnumConsumeStatus callback(Message message);
}
