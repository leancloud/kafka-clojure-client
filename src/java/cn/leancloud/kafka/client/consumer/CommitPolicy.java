package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;
import java.util.concurrent.Future;

public interface CommitPolicy {
    void updatePendingTopicOffset(TopicPartition topicPartition, long offset, Future<HandleRecordSuccess> future);

    void updateCompleteTopicOffset(HandleRecordSuccess success);

    Set<TopicPartition> tryCommit();

    Set<TopicPartition> tryCommitOnClose();

    Set<TopicPartition> tryCommitOnPartitionRevoked();
}
