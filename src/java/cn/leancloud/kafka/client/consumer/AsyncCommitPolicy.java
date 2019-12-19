package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;

final class AsyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCommitPolicy.class);

    private final Consumer<K, V> consumer;
    private final Set<TopicPartition> completePartitions;
    private final int maxConsecutiveAsyncCommits;
    private int consecutiveAsyncCommitCounter;
    private boolean forceSync;

    AsyncCommitPolicy(Consumer<K, V> consumer, int maxConsecutiveAsyncCommits) {
        this.consumer = consumer;
        this.maxConsecutiveAsyncCommits = maxConsecutiveAsyncCommits;
        this.completePartitions = new HashSet<>();
    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {
        completePartitions.add(new TopicPartition(record.topic(), record.partition()));
    }

    @Override
    public Set<TopicPartition> tryCommit(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
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
    public void beforeClose(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        // if there's no pending futures, it means all the messages fetched from broker
        // have processed and we can commit safely
        if (pendingFutures.isEmpty()) {
            consumer.commitSync();
        }

        completePartitions.clear();
    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions, Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        if (pendingFutures.isEmpty()) {
            consumer.commitSync();
        }
    }
}
