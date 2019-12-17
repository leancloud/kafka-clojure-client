package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PendingRecords {
    private Map<TopicPartition, Long> pendingTopicOffset;
    private Set<TopicPartition> completedTopics;

    public PendingRecords() {
        this.pendingTopicOffset = new HashMap<>();
        this.completedTopics = new HashSet<>();
    }

    public void updatePendingTopicOffset(TopicPartition topicPartition, long offset) {
        pendingTopicOffset.merge(topicPartition, offset, Math::max);
    }

    public void updateCompleteTopicOffset(TopicPartition topicPartition, long offset) {
        if (offset >= pendingTopicOffset.get(topicPartition)) {
            completedTopics.add(topicPartition);
        }
    }

    Set<TopicPartition> getAndCleanCompeleteTopics() {
        Set<TopicPartition> topics = new HashSet<>(completedTopics);
        completedTopics.clear();
        return topics;
    }
}
