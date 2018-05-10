package com.zheng.rabbitmq.queuebottleneck.queueindex;

/**
 * 获取消息具体发送到哪一个队列的index加载器
 * @Author zhenglian
 * @Date 2018/5/10 22:56
 */
public interface QueueIndexLoader {
    /**
     * 获取消息发送的队列索引
     * @param msgSeq 消息序号
     * @param subdivision 逻辑队列对应的物理队列数量
     * @return
     */
    int queueIndex(long msgSeq, int subdivision);
}
