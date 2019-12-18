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
    private final Set<TopicPartition> completePartitions;
    private final int maxConsecutiveAsyncCommits;
    private int consecutiveAsyncCommitCounter;
    private boolean forceSync;

    public AsyncCommitPolicy(Consumer<K, V> consumer, int maxConsecutiveAsyncCommits) {
        this.pendingFutures = new HashMap<>();
        this.consumer = consumer;
        this.maxConsecutiveAsyncCommits = maxConsecutiveAsyncCommits;
        this.completePartitions = new HashSet<>();
    }

    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future) {
        pendingFutures.put(record, future);

    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {
        final Future<ConsumerRecord<K, V>> v = pendingFutures.remove(record);
        assert v != null;
        completePartitions.add(new TopicPartition(record.topic(), record.partition()));
    }

    @Override
    public Set<TopicPartition> tryCommit() {
        if (!pendingFutures.isEmpty()) {
            return Collections.emptySet();
        }

        if (forceSync || consecutiveAsyncCommitCounter >= maxConsecutiveAsyncCommits) {
            consumer.commitSync();
            consecutiveAsyncCommitCounter = 0;
            forceSync = false;
        } else {
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    logger.warn("Failed to commit offset: " + offsets + " asynchronously", exception);
                    forceSync = true;
                }
            });
            ++consecutiveAsyncCommitCounter;
        }

        final Set<TopicPartition> partitions = new HashSet<>(completePartitions);
        completePartitions.clear();
        return partitions;
    }

    @Override
    public void beforeClose() {
        // only cancel all the pending futures, but we do not wait them to finish, because
        // this policy uses async commit without specific topic and offset. We can't commit
        // at here otherwise some unprocessed record may falsely be committed.
        for (Future<ConsumerRecord<K, V>> future : pendingFutures.values()) {
            future.cancel(false);
        }

        // if there's no pending futures, it means all the messages fetched from broker
        // have processed and we can commit safely
        if (pendingFutures.isEmpty()) {
            consumer.commitSync();
        }

        pendingFutures.clear();
        completePartitions.clear();
    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions) {
        if (pendingFutures.isEmpty()) {
            consumer.commitSync();
        }
    }
}
