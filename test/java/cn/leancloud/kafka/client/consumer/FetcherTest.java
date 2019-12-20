package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

public class FetcherTest {
    private static final String testingTopic = "TestingTopic";
    private static final Object defaultKey = new Object();
    private static final Object defaultMsg = new Object();
    private static final ConsumerRecord<Object, Object> defaultTestingRecord =
            new ConsumerRecord<>(
                    testingTopic,
                    0,
                    1, defaultKey,
                    defaultMsg);
    private static final long pollTimeout = 100;
    private MockConsumer<Object, Object> consumer;
    private MessageHandler<Object, Object> messageHandler;
    private CommitPolicy<Object, Object> policy;
    private ExecutorService executorService;
    private Fetcher<Object, Object> fetcher;
    private Thread fetcherThread;

    @Before
    public void setUp() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        messageHandler = mock(MessageHandler.class);
        policy = mock(CommitPolicy.class);
        executorService = ImmediateExecutorService.INSTANCE;
    }

    @After
    public void tearDown() throws Exception {
        fetcher.close();
        fetcherThread.join();
        consumer.close();
        executorService.shutdown();
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        executorService = mock(ExecutorService.class);
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, executorService, policy);
        fetcherThread = new Thread(fetcher);

        doNothing().when(executorService).execute(any(Runnable.class));
        assignPartitions(toPartitions(singletonList(0)), 0L);
        consumer.addRecord(defaultTestingRecord);
        fetcherThread.start();

        await().until(() -> !fetcher.pendingFutures().isEmpty());
        assertThat(fetcher.pendingFutures()).hasSize(1).containsOnlyKeys(defaultTestingRecord);
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).beforeClose(any());
    }

    @Test
    public void testHandleMsgFailed() throws Exception {
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, executorService, policy);
        fetcherThread = new Thread(fetcher);

        assignPartitions(toPartitions(singletonList(0)), 0L);
        consumer.addRecord(defaultTestingRecord);
        doThrow(new RuntimeException("expected exception")).when(messageHandler).handleMessage(defaultTestingRecord);

        fetcherThread.start();

        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).beforeClose(any());
        verify(policy, times(1)).addPendingRecord(eq(defaultTestingRecord), any());
        verify(policy, never()).completeRecord(any());
        verify(policy, never()).tryCommit(any());
        verify(messageHandler, times(1)).handleMessage(defaultTestingRecord);
    }

    @Test
    public void testNoPauseWhenMsgHandledFastEnough() throws Exception {
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, executorService, policy);
        fetcherThread = new Thread(fetcher);

        final CyclicBarrier barrier = new CyclicBarrier(2);
        assignPartitions(toPartitions(singletonList(0)), 0L);
        when(policy.tryCommit(anyMap())).thenReturn(Collections.emptySet());
        doAnswer(invocation -> {
            barrier.await();
            return null;
        }).when(policy).completeRecord(defaultTestingRecord);
        consumer.addRecord(defaultTestingRecord);
        fetcherThread.start();

        barrier.await();
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        assertThat(consumer.paused()).isEmpty();
        verify(policy, times(1)).beforeClose(any());
        verify(policy, times(1)).addPendingRecord(eq(defaultTestingRecord), any());
        verify(policy, times(1)).completeRecord(defaultTestingRecord);
        verify(policy, atLeastOnce()).tryCommit(any());
        verify(messageHandler, times(1)).handleMessage(defaultTestingRecord);
    }

    @Test
    public void testPauseResume() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, executors, policy);
        fetcherThread = new Thread(fetcher);

        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        final CyclicBarrier barrier = new CyclicBarrier(pendingRecords.size() + 1);
        final Set<TopicPartition> completePartitions = new HashSet<>();
        assignPartitions(partitions, 0L);

        doAnswer(invocation -> {
            // wait until the main thread figure out that all the partitions was paused
            barrier.await();
            return null;
        }).when(messageHandler).handleMessage(any());

        // record complete partitions
        doAnswer(invocation -> {
            final ConsumerRecord<Object, Object> completeRecord = invocation.getArgument(0);
            completePartitions.add(new TopicPartition(testingTopic, completeRecord.partition()));
            return null;
        }).when(policy).completeRecord(any());

        // resume completed partitions
        when(policy.tryCommit(anyMap())).thenReturn(completePartitions);

        fireConsumerRecords(pendingRecords);

        fetcherThread.start();

        await().until(() -> consumer.paused().size() == pendingRecords.size());
        assertThat(consumer.paused()).isEqualTo(new HashSet<>(partitions));
        // release all the message handler threads
        barrier.await();
        // after the message was handled, the paused partition will be resumed eventually
        await().until(() -> consumer.paused().isEmpty());

        // close and verify
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();

        verify(policy, times(pendingRecords.size())).addPendingRecord(any(), any());
        verify(policy, times(pendingRecords.size())).completeRecord(any());
        verify(policy, times(1)).beforeClose(any());
        verify(messageHandler, times(pendingRecords.size())).handleMessage(any());

        executors.shutdown();
    }

    @Test
    public void testPauseAndPartialResume() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool();
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, executors, policy);
        fetcherThread = new Thread(fetcher);

        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        final CyclicBarrier barrier = new CyclicBarrier(pendingRecords.size() + 1);
        final Set<TopicPartition> completePartitions = new HashSet<>();
        assignPartitions(partitions, 0L);

        doAnswer(invocation -> {
            // wait until the main thread figure out that all the partitions was paused
            barrier.await();
            // only let half of the testing records handled
            final ConsumerRecord<Object, Object> record = invocation.getArgument(0);
            if (record.partition() < pendingRecords.size() / 2) {
                barrier.await();
            }
            return null;
        }).when(messageHandler).handleMessage(any());

        // record complete partitions
        doAnswer(invocation -> {
            final ConsumerRecord<Object, Object> completeRecord = invocation.getArgument(0);
            completePartitions.add(new TopicPartition(testingTopic, completeRecord.partition()));
            return null;
        }).when(policy).completeRecord(any());

        // resume completed partitions
        when(policy.tryCommit(anyMap())).thenReturn(completePartitions);

        fireConsumerRecords(pendingRecords);

        fetcherThread.start();

        await().until(() -> consumer.paused().size() == pendingRecords.size());
        assertThat(consumer.paused()).isEqualTo(new HashSet<>(partitions));
        // release all the message handler threads
        barrier.await();
        // half of the message will be handled and their corresponding partitions will be resumed
        await().until(() -> consumer.paused().size() == pendingRecords.size() / 2);
        assertThat(fetcher.pendingFutures()).hasSize(pendingRecords.size() / 2);
        assertThat(fetcher.pendingFutures().keySet()).containsExactlyInAnyOrderElementsOf(pendingRecords.subList(0, pendingRecords.size() / 2));
        // close and verify
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();

        verify(policy, times(pendingRecords.size())).addPendingRecord(any(), any());
        verify(policy, times(pendingRecords.size() / 2)).completeRecord(any());
        verify(policy, times(1)).beforeClose(any());
        verify(messageHandler, times(pendingRecords.size())).handleMessage(any());

        executors.shutdown();
    }

    private List<TopicPartition> toPartitions(List<Integer> partitions) {
        return partitions
                .stream()
                .map(p -> new TopicPartition(testingTopic, p))
                .collect(toList());
    }

    private void assignPartitions(List<TopicPartition> partitions, long offsets) {
        final Map<TopicPartition, Long> partitionOffset = partitions
                .stream()
                .collect(toMap(Function.identity(), (p) -> offsets));

        consumer.addEndOffsets(partitionOffset);
        consumer.assign(partitionOffset.keySet());
    }

    private List<ConsumerRecord<Object, Object>> prepareConsumerRecords(Collection<TopicPartition> partitions, long offsetStart, int size) {
        final List<ConsumerRecord<Object, Object>> records = new ArrayList<>();

        for (TopicPartition partition : partitions) {
            records.addAll(LongStream.range(offsetStart, offsetStart + size)
                    .boxed()
                    .map(offset -> new ConsumerRecord<>(
                            testingTopic,
                            partition.partition(),
                            offset,
                            defaultKey,
                            defaultMsg))
                    .collect(toList()));

        }

        return records;
    }

    private void fireConsumerRecords(Collection<ConsumerRecord<Object, Object>> records) {
        for (ConsumerRecord<Object, Object> record : records) {
            consumer.addRecord(record);
        }
    }
}