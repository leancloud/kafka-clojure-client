package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Future;

final class AutoCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private static final AutoCommitPolicy INSTANCE = new AutoCommitPolicy();

    @SuppressWarnings("unchecked")
    static <K, V> AutoCommitPolicy<K, V> getInstance() {
        return (AutoCommitPolicy<K, V>) INSTANCE;
    }

    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future) {

    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {

    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        return Collections.emptySet();
    }

    @Override
    public Set<TopicPartition> partialCommit() {
        return Collections.emptySet();
    }
}
