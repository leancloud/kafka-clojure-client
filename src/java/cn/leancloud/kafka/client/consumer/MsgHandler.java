package cn.leancloud.kafka.client.consumer;

public interface MsgHandler<V> {
    void handleMessage(String topic, V value);
}
