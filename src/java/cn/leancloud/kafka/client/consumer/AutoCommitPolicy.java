package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Future;

public final class AutoCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private static final AutoCommitPolicy INSTANCE = new AutoCommitPolicy();

    @SuppressWarnings("unchecked")
    public static <K, V> AutoCommitPolicy<K, V> getInstance() {
        return (AutoCommitPolicy<K, V>) INSTANCE;
    }


    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future) {

    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {

    }

    @Override
    public Set<TopicPartition> tryCommit() {
        return Collections.emptySet();
    }

    @Override
    public void beforeClose() {

    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions) {

    }
}
