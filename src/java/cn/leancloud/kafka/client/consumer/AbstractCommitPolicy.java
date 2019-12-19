package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

abstract class AbstractCommitPolicy<K,V> implements CommitPolicy<K,V> {
    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record, Future<ConsumerRecord<K, V>> future) {

    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {

    }

    @Override
    public Set<TopicPartition> tryCommit(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {
        return Collections.emptySet();
    }

    @Override
    public void beforeClose(Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {

    }

    @Override
    public void onPartitionRevoked(Collection<TopicPartition> partitions, Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures) {

    }
}
