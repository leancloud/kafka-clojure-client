package cn.leancloud.kafka.client.integration;

import cn.leancloud.kafka.client.consumer.LcKafkaConsumer;

public interface ConsumerFactory {
    String getName();
    LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics);
}
