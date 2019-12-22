package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

final class Fetcher<K, V> implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Fetcher.class);

    private final long pollTimeout;
    private final Consumer<K, V> consumer;
    private final MessageHandler<K, V> handler;
    private final ExecutorCompletionService<ConsumerRecord<K, V>> service;
    private final Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures;
    private final CommitPolicy<K, V> policy;
    private volatile boolean closed;

    Fetcher(Consumer<K, V> consumer,
            long pollTimeout,
            MessageHandler<K, V> handler,
            ExecutorService workerPool,
            CommitPolicy<K, V> policy) {
        this.pendingFutures = new HashMap<>();
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
        this.handler = handler;
        this.service = new ExecutorCompletionService<>(workerPool);
        this.policy = policy;
    }

    @Override
    public void run() {
        logger.debug("Fetcher thread started.");
        final long pollTimeout = this.pollTimeout;
        final Consumer<K, V> consumer = this.consumer;
        while (true) {
            try {
                final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);

                if (logger.isDebugEnabled()) {
                    logger.debug("Fetched " + records.count() + " records from: " + records.partitions());
                }

                dispatchFetchedRecords(records);
                processCompletedRecords();

                if (!pendingFutures.isEmpty() && !records.isEmpty()) {
                    consumer.pause(records.partitions());
                }

                tryCommitRecordOffsets();
            } catch (WakeupException ex) {
                if (closed()) {
                    break;
                }
            } catch (Exception ex) {
                close();
                logger.error("Fetcher quit with unexpected exception. Will rebalance after poll timeout.", ex);
                break;
            }
        }

        gracefulShutdown();
    }

    @Override
    public void close() {
        closed = true;
        consumer.wakeup();
    }

    Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures() {
        return pendingFutures;
    }

    private boolean closed() {
        return closed;
    }

    private void dispatchFetchedRecords(ConsumerRecords<K, V> records) {
        final MessageHandler<K, V> handler = this.handler;
        for (ConsumerRecord<K, V> record : records) {
            final Future<ConsumerRecord<K, V>> future = service.submit(() -> {
                handler.handleMessage(record);
                return record;
            });
            pendingFutures.put(record, future);
            policy.addPendingRecord(record, future);
        }
    }

    private void processCompletedRecords() throws InterruptedException, ExecutionException {
        Future<ConsumerRecord<K, V>> f;
        while ((f = service.poll()) != null) {
            assert f.isDone();
            final ConsumerRecord<K, V> r = f.get();
            final Future<ConsumerRecord<K, V>> v = pendingFutures.remove(r);
            assert v != null;
            policy.completeRecord(r);
        }
    }

    private void tryCommitRecordOffsets() {
        final Set<TopicPartition> partitions = policy.tryCommit(pendingFutures.isEmpty());
        if (!partitions.isEmpty()) {
            consumer.resume(partitions);
        }
    }

    private void gracefulShutdown() {
        policy.partialCommit();
        for (Future<ConsumerRecord<K, V>> future : pendingFutures.values()) {
            future.cancel(false);
        }

        pendingFutures.clear();
        logger.debug("Fetcher thread exit.");
    }
}
