package cn.leancloud.kafka.client.consumer;

public interface MessageHandler<V> {
    // Todo: consider to handle the raw ConsumerRecord
    void handleMessage(String topic, V value);
}
