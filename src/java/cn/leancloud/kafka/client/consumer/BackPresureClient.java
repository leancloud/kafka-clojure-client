package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
    private final ExecutorCompletionService<Result> service;
    private final Set<TopicPartition> pausedPartitions;
    private final MsgHandler<V> handler;
    private volatile State state;

    public BackPresureClient(Consumer<K, V> consumer,
                             ExecutorService workerPool,
                             long pollTimeout,
                             MsgHandler<V> handler) {
        this.state = State.INIT;
        this.consumer = consumer;
        this.workerPool = workerPool;
        this.fetcherThread = new Thread(new Fetcher(pollTimeout));
        this.service = new ExecutorCompletionService<>(workerPool);
        this.pausedPartitions = new HashSet<>();
        this.handler = handler;
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

        Fetcher(long pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        @Override
        public void run() {
            final Consumer<K, V> consumer = BackPresureClient.this.consumer;
            int pendingCount = 0;
            while (true) {
                try {
                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    pendingCount += records.count();
                    for (ConsumerRecord<K, V> record : records) {
                        String topic = record.topic();
                        int partition = record.partition();
                        long offset = record.offset();
                        TopicPartition topicPartition = new TopicPartition(topic, partition);
                        pausedPartitions.add(topicPartition);


                        service.submit(() -> {
                            handler.handleMessage(topic, record.value());
                            return new Result(topicPartition, offset);
                        });

                        consumer.commitAsync();
                    }

                    consumer.pause(pausedPartitions);


                    Future<Result> f;
                    while ((f = service.poll()) != null) {
                        try {
                            Result r = f.get();
                        } catch (InterruptedException | ExecutionException e) {

                        }

                        --pendingCount;
                    }


                    if (pendingCount == 0) {
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
        }
    }

    private static class Result {
        private TopicPartition topicPartition;
        private Long offset;

        public Result(TopicPartition topicPartition, Long offset) {
            this.topicPartition = topicPartition;
            this.offset = offset;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public Long offset() {
            return offset;
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
