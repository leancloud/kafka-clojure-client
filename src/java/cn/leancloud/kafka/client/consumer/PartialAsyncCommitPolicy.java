package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

final class PartialAsyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(PartialAsyncCommitPolicy.class);

    private final int maxConsecutiveAsyncCommits;
    private int consecutiveAsyncCommitCounter;
    private boolean forceSync;

    PartialAsyncCommitPolicy(Consumer<K, V> consumer, int maxConsecutiveAsyncCommits) {
        super(consumer);
        this.maxConsecutiveAsyncCommits = maxConsecutiveAsyncCommits;
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        }

        if (forceSync || consecutiveAsyncCommitCounter >= maxConsecutiveAsyncCommits) {
            consumer.commitSync(completedTopicOffsets);
            consecutiveAsyncCommitCounter = 0;
            forceSync = false;
            final Set<TopicPartition> partitions = checkCompletedPartitions();
            completedTopicOffsets.clear();
            for (TopicPartition p : partitions) {
                topicOffsetHighWaterMark.remove(p);
            }
            return partitions;
        } else {
            consumer.commitAsync(completedTopicOffsets, (offsets, exception) -> {
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                    completedTopicOffsets.remove(entry.getKey(), entry.getValue());
                    topicOffsetHighWaterMark.remove(entry.getKey(), entry.getValue().offset());
                }

                if (exception != null) {
                    logger.warn("Failed to commit offset: " + offsets + " asynchronously", exception);
                    forceSync = true;
                }
            });
            ++consecutiveAsyncCommitCounter;

            return checkCompletedPartitions();
        }
    }
}
