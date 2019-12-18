package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toSet;

public final class PartialSyncCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private final Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures;
    private final Consumer<K, V> consumer;
    private final Map<TopicPartition, Long> topicOffsetHighWaterMark;
    private final Map<TopicPartition, OffsetAndMetadata> completedTopicOffsets;

    public PartialSyncCommitPolicy(Consumer<K, V> consumer) {
        this.pendingFutures = new HashMap<>();
        this.consumer = consumer;
        this.topicOffsetHighWaterMark = new HashMap<>();
        this.completedTopicOffsets = new HashMap<>();
    }

    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future) {
        pendingFutures.put(record, future);
        topicOffsetHighWaterMark.merge(
                new TopicPartition(record.topic(), record.partition()),
                record.offset() + 1,
                Math::max);
    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {
        final Future<ConsumerRecord<K, V>> v = pendingFutures.remove(record);
        assert v != null;
        completedTopicOffsets.merge(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1L),
                (offset1, offset2) -> offset1.offset() > offset2.offset() ? offset1 : offset2);
    }

    @Override
    public Set<TopicPartition> tryCommit() {
        if (completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        }

        consumer.commitSync(completedTopicOffsets);
        return clearCompletedTopics();
    }

    @Override
    public void beforeClose() {
        for (Future<ConsumerRecord<K, V>> future : pendingFutures.values()) {
            future.cancel(false);
        }
        consumer.commitSync(completedTopicOffsets);
        pendingFutures.clear();
        completedTopicOffsets.clear();
        topicOffsetHighWaterMark.clear();
    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions) {
        // the offset out of revoked partitions will be committed twice
        // but I think it's OK
        consumer.commitSync(completedTopicOffsets);

        for (TopicPartition topicPartition : partitions) {
            topicOffsetHighWaterMark.remove(topicPartition);
            completedTopicOffsets.remove(topicPartition);
        }
    }

    private Set<TopicPartition> clearCompletedTopics() {
        final Set<TopicPartition> completedTopics = completedTopicOffsets
                .entrySet()
                .stream()
                .filter(entry -> topicOffsetMeetHighWaterMark(entry.getKey(), entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(toSet());

        completedTopicOffsets.clear();
        return completedTopics;
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
