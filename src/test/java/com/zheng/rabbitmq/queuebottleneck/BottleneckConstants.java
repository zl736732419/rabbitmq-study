package com.zheng.rabbitmq.queuebottleneck;

/**
 * @Author zhenglian
 * @Date 2018/5/11 9:22
 */
public class BottleneckConstants {
    // 一个逻辑队列对应的物理队列数
    public static final Integer SUBDIVISION = 4;
    public static final String QUEUE = "queue";
    public static final String EXCHANGE = "exchange";
    public static final String ROUTING_KEY = "rk";
}
