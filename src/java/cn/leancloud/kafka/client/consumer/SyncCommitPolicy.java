package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.Future;

public final class SyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private final Consumer<K, V> consumer;
    private final Set<TopicPartition> completeTopicPartitions;

    public SyncCommitPolicy(Consumer<K, V> consumer) {
        this.consumer = consumer;
        this.completeTopicPartitions = new HashSet<>();
    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {
        completeTopicPartitions.add(new TopicPartition(record.topic(), record.partition()));
    }

    @Override
    public Set<TopicPartition> tryCommit(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        if (pendingFutures.isEmpty()) {
            consumer.commitSync();
            final Set<TopicPartition> completePartitions = new HashSet<>(completeTopicPartitions);
            completeTopicPartitions.clear();
            return completePartitions;
        }
        return Collections.emptySet();
    }

    @Override
    public void beforeClose(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        tryCommit(pendingFutures);
    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions, Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        tryCommit(pendingFutures);
    }
}
