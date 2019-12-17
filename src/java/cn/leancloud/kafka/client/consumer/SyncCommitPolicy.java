package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public class SyncCommitPolicy<K, V> implements CommitPolicy {
    private final Map<HandleRecordSuccess, Future<HandleRecordSuccess>> pendingFutures;
    private final Consumer<K, V> consumer;

    public SyncCommitPolicy(Consumer<K, V> consumer) {
        this.pendingFutures = new HashMap<>();
        this.consumer = consumer;
    }

    @Override
    public void updatePendingTopicOffset(TopicPartition topicPartition, long offset, Future<HandleRecordSuccess> future) {

    }

    @Override
    public void updateCompleteTopicOffset(HandleRecordSuccess success) {

    }

    @Override
    public Set<TopicPartition> tryCommit() {
        return null;
    }

    @Override
    public Set<TopicPartition> tryCommitOnClose() {
        return null;
    }

    @Override
    public Set<TopicPartition> tryCommitOnPartitionRevoked() {
        return null;
    }
}
