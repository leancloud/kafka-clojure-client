package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

public class HandleRecordSuccess {
    private final TopicPartition topicPartition;
    private final Long processedRecordOffset;

    HandleRecordSuccess(TopicPartition topicPartition, long processedRecordOffset) {
        this.topicPartition = topicPartition;
        this.processedRecordOffset = processedRecordOffset;
    }

    TopicPartition topicPartition() {
        return topicPartition;
    }

    long processedRecordOffset() {
        return processedRecordOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final HandleRecordSuccess that = (HandleRecordSuccess) o;
        return Objects.equals(topicPartition, that.topicPartition) &&
                Objects.equals(processedRecordOffset, that.processedRecordOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, processedRecordOffset);
    }

    @Override
    public String toString() {
        return "HandleRecordSuccess{" +
                "topicPartition=" + topicPartition +
                ", processedRecordOffset=" + processedRecordOffset +
                '}';
    }
}
