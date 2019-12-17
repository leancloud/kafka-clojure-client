package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.Future;

public class SyncCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private final Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures;
    private final Consumer<K, V> consumer;
    private final Set<TopicPartition> completeTopicPartitions;

    public SyncCommitPolicy(Consumer<K, V> consumer) {
        this.pendingFutures = new HashMap<>();
        this.consumer = consumer;
        this.completeTopicPartitions = new HashSet<>();
    }

    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future) {
        pendingFutures.put(record, future);
    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {
        Future<ConsumerRecord<K, V>> v = pendingFutures.remove(record);
        assert v != null;
        completeTopicPartitions.add(new TopicPartition(record.topic(), record.partition()));
    }

    @Override
    public Set<TopicPartition> tryCommit() {
        if (pendingFutures.isEmpty()) {
            consumer.commitSync();
            return completeTopicPartitions;
        }
        return Collections.emptySet();
    }

    @Override
    public void beforeClose() {
        // only cancel all the pending futures, but we do not wait them to finish, because
        // this policy uses sync commit without specific topic and offset. We can't sync commit
        // at here otherwise some unprocessed record may falsely be committed.
        for (Future<ConsumerRecord<K, V>> future : pendingFutures.values()) {
            future.cancel(false);
        }
    }

    @Override
    public void onPartitionRevoked() {
    }
}
