package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static cn.leancloud.kafka.client.consumer.TestingUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

public class SyncCommitPolicyTest {
    private static final String testingTopic = "TestingTopic";
    private static final Object defaultKey = new Object();
    private static final Object defaultMsg = new Object();

    private MockConsumer<Object, Object> consumer;
    private SyncCommitPolicy<Object, Object> policy;

    @Before
    public void setUp() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new SyncCommitPolicy<>(consumer);
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
    }

    @Test
    public void testTryCommitWhenThereExistsPendingRecords() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.completeRecord(record);
        }
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
    }

    @Test
    public void testTryCommitAll() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.completeRecord(record);
        }
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
    }

    @Test
    public void testTryCommitWhenNoCompleteRecords() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
    }

    @Test
    public void testTryCommitBeforeClose() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.completeRecord(record);
        }
        policy.partialCommit();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
    }

    @Test
    public void testTryCommitBeforeClose2() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.completeRecord(record);
        }
        policy.partialCommit();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
    }

}