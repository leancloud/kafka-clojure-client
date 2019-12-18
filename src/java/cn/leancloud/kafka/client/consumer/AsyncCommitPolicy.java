package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;

public class AsyncCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCommitPolicy.class);

    private final Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures;
    private final Consumer<K, V> consumer;
    private final Set<TopicPartition> completeTopicPartitions;

    public AsyncCommitPolicy(Consumer<K, V> consumer) {
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
        final Future<ConsumerRecord<K, V>> v = pendingFutures.remove(record);
        assert v != null;
        completeTopicPartitions.add(new TopicPartition(record.topic(), record.partition()));
    }

    @Override
    public Set<TopicPartition> tryCommit() {
        if (pendingFutures.isEmpty()) {
            consumer.commitAsync((offsets, exception) -> {
                // we use commitAsync to commit offset, so we don't know where we have committed.
                // If we retry the failed commit here, we may encounter out of order error. Because
                // when we retry, there could be some greater offsets have committed to broker after this failed commit.
                // So we can only log the failed commit here.
                logger.warn("Failed to commit offset: " + offsets + " asynchronously", exception);
            });
            return completeTopicPartitions;
        }
        return Collections.emptySet();
    }

    @Override
    public void beforeClose() {
        // only cancel all the pending futures, but we do not wait them to finish, because
        // this policy uses async commit without specific topic and offset. We can't commit
        // at here otherwise some unprocessed record may falsely be committed.
        for (Future<ConsumerRecord<K, V>> future : pendingFutures.values()) {
            future.cancel(false);
        }
        pendingFutures.clear();
    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions) {
    }
}
