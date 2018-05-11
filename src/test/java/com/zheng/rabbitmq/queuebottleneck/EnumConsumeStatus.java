package com.zheng.rabbitmq.queuebottleneck;

import java.util.Objects;

/**
 * 消息消费状态
 * @Author zhenglian
 * @Date 2018/5/11 11:54
 */
public enum EnumConsumeStatus {
    SUCCESS(1, "消费成功"),
    FAILED(0, "消费失败");

    private Integer key;
    private String value;

    EnumConsumeStatus(Integer key, String value) {
        this.key = key;
        this.value = value;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    
    public EnumConsumeStatus findByKey(Integer key) {
        for(EnumConsumeStatus status : EnumConsumeStatus.values()) {
            if (Objects.equals(key, status.key)) {
                return status;
            }
        }
        return null;
    }
    
}
