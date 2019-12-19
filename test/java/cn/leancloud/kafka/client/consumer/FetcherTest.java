package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

public class FetcherTest {
    private static final String testingTopic = "TestingTopic";
    private static final long pollTimeout = 100;
    private MockConsumer<Object, Object> consumer;
    private MessageHandler<Object> messageHandler;
    private CommitPolicy<Object, Object> policy;
    private ExecutorService executorService;
    private Fetcher<Object, Object> fetcher;
    private Thread fetcherThread;

    @Before
    public void setUp() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        messageHandler = mock(MessageHandler.class);
        policy = mock(CommitPolicy.class);
    }

    @After
    public void tearDown() throws Exception {
        fetcher.close();
        fetcherThread.join();
        consumer.close();
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        executorService = mock(ExecutorService.class);
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, executorService, policy);
        fetcherThread = new Thread(fetcher);

        final Future<ConsumerRecord<Object, Object>> neverDoneFuture = mock(Future.class);
        final Object testingMsg = new Object();
        final ConsumerRecord<Object, Object> pendingRecord = new ConsumerRecord<>(testingTopic, 0, 1, new Object(), testingMsg);
        when(executorService.submit(any(Runnable.class))).thenAnswer(invocationOnMock -> {
            Callable<ConsumerRecord<Object, Object>> job = invocationOnMock.getArgument(0);
            job.call();
            return neverDoneFuture;
        });
        assignPartitions(Collections.singletonList(0), Collections.singletonList(0L));
        consumer.addRecord(pendingRecord);
        fetcherThread.start();

        await().until(() -> !fetcher.pendingFutures().isEmpty());
        assertThat(fetcher.pendingFutures()).hasSize(1).containsOnlyKeys(pendingRecord).containsValue(neverDoneFuture);
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).beforeClose(any());
        verify(messageHandler, times(1)).handleMessage(testingTopic, testingMsg);
    }

    @Test
    public void testHandleMsgFailed() throws Exception {
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, ImmediateExecutorService.INSTANCE, policy);
        fetcherThread = new Thread(fetcher);

        final Object testingMsg = new Object();
        final ConsumerRecord<Object, Object> pendingRecord = new ConsumerRecord<>(testingTopic, 0, 1, new Object(), testingMsg);
        assignPartitions(Collections.singletonList(0), Collections.singletonList(0L));
        consumer.addRecord(pendingRecord);
        fetcherThread.start();
        doThrow(new RuntimeException("expected exception")).when(messageHandler).handleMessage(testingTopic, testingMsg);

        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).beforeClose(any());
        verify(messageHandler, times(1)).handleMessage(testingTopic, testingMsg);
    }

    @Test
    public void testNoPauseWhenMsgHandledFastEnough() throws Exception {
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, ImmediateExecutorService.INSTANCE, policy);
        fetcherThread = new Thread(fetcher);

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Object testingMsg = new Object();
        final ConsumerRecord<Object, Object> pendingRecord = new ConsumerRecord<>(testingTopic, 0, 1, new Object(), testingMsg);
        assignPartitions(Collections.singletonList(0), Collections.singletonList(0L));
        when(policy.tryCommit(anyMap())).thenReturn(Collections.emptySet());
        doAnswer(invocation -> {
            barrier.await();
            return null;
        }).when(policy).completeRecord(pendingRecord);
        consumer.addRecord(pendingRecord);
        fetcherThread.start();

        barrier.await();
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        assertThat(consumer.paused()).isEmpty();
        verify(policy, times(1)).beforeClose(any());
        verify(messageHandler, times(1)).handleMessage(testingTopic, testingMsg);
    }

    @Test
    public void testPauseResume() throws Exception {
        ExecutorService executors = Executors.newCachedThreadPool();
        fetcher = new Fetcher<>(consumer, pollTimeout, messageHandler, executors, policy);
        fetcherThread = new Thread(fetcher);

        final List<Integer> partitions = IntStream.range(0, 30).boxed().collect(Collectors.toList());


        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Object testingMsg = new Object();
        final ConsumerRecord<Object, Object> pendingRecord = new ConsumerRecord<>(testingTopic, 0, 1, new Object(), testingMsg);
        assignPartitions(partitions, Collections.singletonList(0L));

        doAnswer(invocation -> {
            barrier.await();
            return null;
        }).when(messageHandler).handleMessage(testingTopic, testingMsg);

        when(policy.tryCommit(anyMap())).thenReturn(Collections.emptySet());
        doAnswer(invocation -> {
            barrier.await();
            return null;
        }).when(policy).completeRecord(pendingRecord);
        consumer.addRecord(pendingRecord);
        fetcherThread.start();

        barrier.await();
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        assertThat(consumer.paused()).isEmpty();
        verify(policy, times(1)).beforeClose(any());
        verify(messageHandler, times(1)).handleMessage(testingTopic, testingMsg);

        executors.shutdown();
    }

    private void assignPartitions(List<Integer> partitions, List<Long> offsets) {
        if (partitions.size() != offsets.size()) {
            throw new IllegalArgumentException("partitions and offsets'size should be equal.");
        }

        Map<TopicPartition, Long> partitionOffset = new HashMap<>();
        for (int i = 0; i < partitions.size(); i++) {
            partitionOffset.put(new TopicPartition(testingTopic, partitions.get(i)), offsets.get(i));
        }

        consumer.addEndOffsets(partitionOffset);
        consumer.assign(partitionOffset.keySet());
    }
}