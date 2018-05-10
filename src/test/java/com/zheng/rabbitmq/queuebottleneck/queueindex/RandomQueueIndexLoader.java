package com.zheng.rabbitmq.queuebottleneck.queueindex;

import java.util.Random;

/**
 * 随机队列index loader,用于将消息随机发送到给定的物理队列
 * 消费者不关心消息消费的顺序
 * @Author zhenglian
 * @Date 2018/5/10 22:57
 */
public class RandomQueueIndexLoader implements QueueIndexLoader {
    @Override
    public int queueIndex(long msgSeq, int subdivision) {
        Random random = new Random();
        return random.nextInt(subdivision);
    }
}
