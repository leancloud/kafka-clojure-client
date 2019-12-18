package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public final class RebalanceListener<K, V> implements ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    private final Fetcher<K, V> fetcher;
    private final CommitPolicy<K, V> policy;

    RebalanceListener(Fetcher<K, V> fetcher, CommitPolicy<K, V> policy) {
        this.fetcher = fetcher;
        this.policy = policy;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partitions was revoked {}", partitions);

        policy.onPartitionRevoked(partitions, fetcher.pendingFutures());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partitions was assigned {}", partitions);
    }
}
