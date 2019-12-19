package cn.leancloud.kafka.client.consumer;

public interface MessageHandler<V> {
    void handleMessage(String topic, V value);
}
