package com.zheng.rabbitmq.queuebottleneck;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 对象序列化工具类
 * 采用google protostuff进行对象的序列与反序列化操作
 * Created by zhenglian on 2017/10/16.
 */
public class SerializationUtil {
    /**
     * schema集合
     */
    public static Map<Class<?>, Schema<?>> cacheSchemaMap = new ConcurrentHashMap<>();

    /**
     * 获取类描述
     * @param clazz
     * @return
     */
    private static <T> Schema<T> getSchema(Class<T> clazz) {
        Schema<T> schema = (Schema<T>) cacheSchemaMap.get(clazz);
        if (!Optional.ofNullable(schema).isPresent()) {
            schema = RuntimeSchema.getSchema(clazz);
            cacheSchemaMap.put(clazz, schema);
        }
        return schema;
    }

    /**
     * 序列化对象成字节数组
     * @param message
     * @param <T>
     * @return
     */
    public static <T> byte[] serialize(T message) {
        Class<T> clazz = (Class<T>) message.getClass();
        Schema<T> schema = getSchema(clazz);
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] bytes = ProtostuffIOUtil.toByteArray(message, schema, buffer);
        buffer.clear(); // 清空临时缓存
        return bytes;
    }

    /**
     * 反序列化
     * @param bytes
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            T message = clazz.newInstance();
            Schema<T> schema = getSchema(clazz);
            ProtostuffIOUtil.mergeFrom(bytes, message, schema);
            return message;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}