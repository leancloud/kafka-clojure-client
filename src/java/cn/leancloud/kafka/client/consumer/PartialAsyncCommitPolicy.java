package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toSet;
import static java.util.function.BinaryOperator.maxBy;

final class PartialAsyncCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(PartialAsyncCommitPolicy.class);

    private final Consumer<K, V> consumer;
    private final Map<TopicPartition, Long> topicOffsetHighWaterMark;
    private final Map<TopicPartition, OffsetAndMetadata> completedTopicOffsets;
    private final int maxConsecutiveAsyncCommits;
    private int consecutiveAsyncCommitCounter;
    private boolean forceSync;

    PartialAsyncCommitPolicy(Consumer<K, V> consumer, int maxConsecutiveAsyncCommits) {
        this.consumer = consumer;
        this.maxConsecutiveAsyncCommits = maxConsecutiveAsyncCommits;
        this.topicOffsetHighWaterMark = new HashMap<>();
        this.completedTopicOffsets = new HashMap<>();
    }

    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future) {
        topicOffsetHighWaterMark.merge(
                new TopicPartition(record.topic(), record.partition()),
                record.offset() + 1,
                Math::max);
    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {
        completedTopicOffsets.merge(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1L),
                maxBy(comparing(OffsetAndMetadata::offset)));
    }

    @Override
    public Set<TopicPartition> tryCommit(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
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

    @Override
    public void beforeClose(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        consumer.commitSync(completedTopicOffsets);
        completedTopicOffsets.clear();
        topicOffsetHighWaterMark.clear();
    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions, Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        // the offset out of revoked partitions will be committed twice
        // but I think it's OK
        consumer.commitSync(completedTopicOffsets);

        for (TopicPartition topicPartition : partitions) {
            topicOffsetHighWaterMark.remove(topicPartition);
            completedTopicOffsets.remove(topicPartition);
        }
    }

    private Set<TopicPartition> checkCompletedPartitions() {
        return completedTopicOffsets
                .entrySet()
                .stream()
                .filter(entry -> topicOffsetMeetHighWaterMark(entry.getKey(), entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(toSet());
    }

    private boolean topicOffsetMeetHighWaterMark(TopicPartition topicPartition, OffsetAndMetadata offset) {
        final Long offsetHighWaterMark = topicOffsetHighWaterMark.get(topicPartition);
        if (offsetHighWaterMark != null) {
            return offset.offset() >= offsetHighWaterMark;
        }
        // maybe this partition revoked before a msg of this partition was processed
        return true;
    }
}
