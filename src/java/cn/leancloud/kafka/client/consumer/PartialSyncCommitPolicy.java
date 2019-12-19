package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.Future;

import static java.util.Comparator.comparing;
import static java.util.function.BinaryOperator.maxBy;
import static java.util.stream.Collectors.toSet;

final class PartialSyncCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private final Consumer<K, V> consumer;
    private final Map<TopicPartition, Long> topicOffsetHighWaterMark;
    private final Map<TopicPartition, OffsetAndMetadata> completedTopicOffsets;

    PartialSyncCommitPolicy(Consumer<K, V> consumer) {
        this.consumer = consumer;
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

        consumer.commitSync(completedTopicOffsets);
        final Set<TopicPartition> partitions = checkCompletedPartitions();
        completedTopicOffsets.clear();
        for (TopicPartition p : partitions) {
            topicOffsetHighWaterMark.remove(p);
        }
        return partitions;
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
