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
import java.util.concurrent.ExecutionException;
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
    private final Fetcher fetcher;
    private final ExecutorCompletionService<HandleRecordSuccess> service;
    private volatile State state;

    public BackPresureClient(Consumer<K, V> consumer,
                             ExecutorService workerPool,
                             long pollTimeout,
                             MsgHandler<V> handler) {
        this.state = State.INIT;
        this.consumer = consumer;
        this.workerPool = workerPool;
        this.fetcher = new Fetcher(pollTimeout, handler);
        this.fetcherThread = new Thread();

        this.service = new ExecutorCompletionService<>(workerPool);
    }

    public synchronized void subscribe(Collection<String> topics, boolean consumeFromLargest) {
        if (subscribed() || closed()) {
            throw new IllegalStateException("client is in " + state + " state. expect: " + State.INIT);
        }

        consumer.subscribe(topics, new RebalanceListener(fetcher));
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
        private final Set<TopicPartition> pausedPartitions;

        Fetcher(long pollTimeout, MsgHandler<V> handler) {
            this.pollTimeout = pollTimeout;
            this.handler = handler;
            this.pendingFutures = new HashMap<>();
            this.pausedPartitions = new HashSet<>();
        }

        @Override
        public void run() {
            final Consumer<K, V> consumer = BackPresureClient.this.consumer;
            while (true) {
                try {
                    final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    if (!records.isEmpty()) {
                        dispatchFetchedRecords(records);
                        consumer.pause(pausedPartitions);
                    }

                    processCompleteRecords();
                    tryCommitRecordOffsets();
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

        Set<TopicPartition> pausedPartitions() {
            return pausedPartitions;
        }

        private void dispatchFetchedRecords(ConsumerRecords<K, V> records) {
            final MsgHandler<V> handler = this.handler;
            for (ConsumerRecord<K, V> record : records) {
                final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                pausedPartitions.add(topicPartition);

                final HandleRecordSuccess resp = new HandleRecordSuccess(topicPartition, record.offset());
                pendingFutures.put(resp, service.submit(() -> {
                    handler.handleMessage(record.topic(), record.value());
                    return resp;
                }));
            }
        }

        private void processCompleteRecords() throws InterruptedException, ExecutionException {
            final Map<HandleRecordSuccess, Future<HandleRecordSuccess>> pendingFutures = this.pendingFutures;
            Future<HandleRecordSuccess> f;
            while ((f = service.poll()) != null) {
                assert f.isDone();
                HandleRecordSuccess r = f.get();
                pendingFutures.remove(r);
            }
        }

        private void tryCommitRecordOffsets() {
            if (pendingFutures.isEmpty()) {
                consumer.commitSync();
                consumer.resume(pausedPartitions);
                pausedPartitions.clear();
            }
        }

        private void cancelAllPendingFutures() {
            for (Future<HandleRecordSuccess> future : pendingFutures.values()) {
                future.cancel(false);
            }
        }
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        private final Fetcher fetcher;

        RebalanceListener(Fetcher fetcher) {
            this.fetcher = fetcher;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("Partitions was revoked {}", partitions);

            fetcher.pausedPartitions().removeAll(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("Partitions was assigned {}", partitions);
        }
    }
}
