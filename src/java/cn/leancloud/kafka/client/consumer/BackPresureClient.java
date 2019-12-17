package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BackPresureClient<K, V> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(BackPresureClient.class.getSimpleName());

    private enum State {
        INIT(0),
        SUBSCRIBED(1),
        CLOSED(2);

        private int code;

        State(int code) {
            this.code = code;
        }

        int code() {
            return code;
        }
    }

    private final Consumer<K, V> consumer;
    private final ExecutorService workerPool;
    private final Thread fetcherThread;
    private final ExecutorCompletionService<HandleRecordSuccess> service;
    private final Set<TopicPartition> pausedPartitions;
    private volatile State state;

    public BackPresureClient(Consumer<K, V> consumer,
                             ExecutorService workerPool,
                             long pollTimeout,
                             MsgHandler<V> handler) {
        this.state = State.INIT;
        this.consumer = consumer;
        this.workerPool = workerPool;
        this.fetcherThread = new Thread(new Fetcher(pollTimeout, handler));
        this.service = new ExecutorCompletionService<>(workerPool);
        this.pausedPartitions = new HashSet<>();
    }

    public synchronized void subscribe(Collection<String> topics, boolean consumeFromLargest) {
        if (subscribed() || closed()) {
            throw new IllegalStateException("client is in " + state + " state. expect: " + State.INIT);
        }

        consumer.subscribe(topics, new RebalanceListener(pausedPartitions));
        if (consumeFromLargest) {
            consumer.seekToEnd(Collections.emptyList());
        }

        fetcherThread.run();
        state = State.SUBSCRIBED;
    }

    @Override
    public synchronized void close() {
        state = State.CLOSED;
        consumer.close();
    }

    private boolean subscribed() {
        return state.code() > State.INIT.code();
    }

    private boolean closed() {
        return state == State.CLOSED;
    }

    private class Fetcher implements Runnable {
        private final long pollTimeout;
        private final MsgHandler<V> handler;
        private final Map<HandleRecordSuccess, Future<HandleRecordSuccess>> pendingFutures;

        Fetcher(long pollTimeout, MsgHandler<V> handler) {
            this.pollTimeout = pollTimeout;
            this.handler = handler;
            this.pendingFutures = new HashMap<>();
        }

        @Override
        public void run() {
            final Consumer<K, V> consumer = BackPresureClient.this.consumer;
            final MsgHandler<V> handler = this.handler;
            PendingRecords pendingRecords = new PendingRecords();
            final Map<HandleRecordSuccess, Future<HandleRecordSuccess>> pendingFutures = this.pendingFutures;
            while (true) {
                try {
                    final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    for (ConsumerRecord<K, V> record : records) {
                        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        pendingRecords.updatePendingTopicOffset(topicPartition, record.offset());

                        pausedPartitions.add(topicPartition);

                        final HandleRecordSuccess resp = new HandleRecordSuccess(topicPartition, record.offset());
                        pendingFutures.put(resp, service.submit(() -> {
                            handler.handleMessage(record.topic(), record.value());
                            return resp;
                        }));
                    }

                    if (!records.isEmpty()) {
                        consumer.pause(pausedPartitions);
                    }

                    Future<HandleRecordSuccess> f;
                    while ((f = service.poll()) != null) {
                        assert f.isDone();
                        HandleRecordSuccess r = f.get();
                        pendingFutures.remove(r);
                        pendingRecords.updateCompleteTopicOffset(r.topicPartition(), r.processedRecordOffset());
                    }

                    if (pendingFutures.isEmpty()) {
                        consumer.commitSync();
                        consumer.resume(pausedPartitions);
                        pausedPartitions.clear();
                    }
                } catch (WakeupException ex) {
                    if (closed()) {
                        break;
                    }
                } catch (Exception ex) {
                    if (closed()) {
                        break;
                    }

                    logger.error("Fetcher quit with unexpected exception. Will rebalance after poll timeout.", ex);
                }
            }

            cancelAllPendingFutures();
        }

        private void cancelAllPendingFutures() {
            for (Future<HandleRecordSuccess> future : pendingFutures.values()) {
                future.cancel(false);
            }
        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener {
        private final Set<TopicPartition> pausedPartitions;

        RebalanceListener(Set<TopicPartition> pausedPartitions) {
            this.pausedPartitions = pausedPartitions;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("Partitions was revoked {}", partitions);

            pausedPartitions.removeAll(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("Partitions was assigned {}", partitions);
        }
    }
}
