package com.zheng.rabbitmq.queuebottleneck.queueindex;

/**
 * 取模算法，根据消息序号与物理队列数取模，得到最终的队列索引
 * 这种方式要求消息发送者设置的消息序号是连续有序的
 * 否则无法保证消息的顺序消费
 * @Author zhenglian
 * @Date 2018/5/10 23:01
 */
public class RoundRobinQueueIndexLoader implements QueueIndexLoader {
    @Override
    public int queueIndex(long msgSeq, int subdivision) {
        int index = (int) msgSeq % subdivision;
        return index;
    }
}
