package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class RebalanceListener<K, V> implements ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    private final CommitPolicy<K, V> policy;
    private final Consumer<K,V> consumer;
    private Set<TopicPartition> pausedPartitions;

    RebalanceListener(Consumer<K, V> consumer, CommitPolicy<K, V> policy) {
        this.policy = policy;
        this.consumer = consumer;
        this.pausedPartitions = Collections.emptySet();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partitions was revoked {}", partitions);

        pausedPartitions = consumer.paused();
        pausedPartitions.removeAll(policy.partialCommit());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partitions was assigned {}", partitions);

        if (!pausedPartitions.isEmpty()) {
            consumer.pause(pausedPartitions);
        }
    }
}
