package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import static java.util.Comparator.comparing;
import static java.util.function.BinaryOperator.maxBy;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

final class SyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    SyncCommitPolicy(Consumer<K, V> consumer) {
        super(consumer);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (noPendingRecords && !completedTopicOffsets.isEmpty()) {
            consumer.commitSync();
            final Set<TopicPartition> completePartitions = new HashSet<>(completedTopicOffsets.keySet());
            completedTopicOffsets.clear();
            topicOffsetHighWaterMark.clear();
            return completePartitions;
        }
        return Collections.emptySet();
    }
}
