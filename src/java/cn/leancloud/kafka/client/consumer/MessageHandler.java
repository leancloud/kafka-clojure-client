package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageHandler<K, V> {
    void handleMessage(ConsumerRecord<K, V> record);
}
