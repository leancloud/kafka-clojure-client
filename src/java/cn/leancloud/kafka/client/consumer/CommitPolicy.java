package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public interface CommitPolicy<K, V> {
    void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future);

    void completeRecord(ConsumerRecord<K, V> record);

    Set<TopicPartition> tryCommit(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures);

    void beforeClose(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures);

    void onPartitionRevoked(Collection<TopicPartition> partitions, Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures);
}
