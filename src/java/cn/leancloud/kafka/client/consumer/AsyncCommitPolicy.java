package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class AsyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCommitPolicy.class);

    private final int maxConsecutiveAsyncCommits;
    private int consecutiveAsyncCommitCounter;
    private boolean forceSync;

    AsyncCommitPolicy(Consumer<K, V> consumer, int maxConsecutiveAsyncCommits) {
        super(consumer);
        this.maxConsecutiveAsyncCommits = maxConsecutiveAsyncCommits;
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (!noPendingRecords) {
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

        final Set<TopicPartition> partitions = new HashSet<>(completedTopicOffsets.keySet());
        completedTopicOffsets.clear();
        return partitions;
    }
}
