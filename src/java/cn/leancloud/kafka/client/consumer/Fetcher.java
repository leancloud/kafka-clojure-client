package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

class Fetcher<K, V> implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Fetcher.class);

    private final long pollTimeout;
    private final Consumer<K, V> consumer;
    private final MsgHandler<V> handler;
    private final ExecutorCompletionService<ConsumerRecord<K, V>> service;
    private volatile boolean closed;
    private CommitPolicy<K, V> policy;

    Fetcher(Consumer<K, V> consumer,
            long pollTimeout,
            MsgHandler<V> handler,
            ExecutorCompletionService<ConsumerRecord<K, V>> service,
            CommitPolicy<K, V> policy) {
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
        this.handler = handler;
        this.service = service;
        this.policy = policy;
    }

    @Override
    public void run() {
        logger.debug("Fetcher thread started.");
        final Consumer<K, V> consumer = this.consumer;
        while (true) {
            try {
                final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);

                if (logger.isDebugEnabled()) {
                    logger.debug("Fetched " + records.count() + " records from: " + records.partitions());
                }

                if (!records.isEmpty()) {
                    dispatchFetchedRecords(records);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Pause partitions: " + records.partitions());
                    }

                    consumer.pause(records.partitions());
                }

                processCompletedRecords();

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

        policy.beforeClose();
        logger.debug("Fetcher thread exit.");
    }

    @Override
    public void close() {
        closed = true;
        consumer.wakeup();
    }

    private boolean closed() {
        return closed;
    }

    private void dispatchFetchedRecords(ConsumerRecords<K, V> records) {
        final MsgHandler<V> handler = this.handler;
        for (ConsumerRecord<K, V> record : records) {
            final Future<ConsumerRecord<K, V>> future = service.submit(() -> {
                handler.handleMessage(record.topic(), record.value());
                return record;
            });
            policy.addPendingRecord(record, future);
        }
    }

    private void processCompletedRecords() throws InterruptedException, ExecutionException {
        Future<ConsumerRecord<K, V>> f;
        while ((f = service.poll()) != null) {
            assert f.isDone();
            final ConsumerRecord<K, V> r = f.get();
            policy.completeRecord(r);
        }
    }

    private void tryCommitRecordOffsets() {
        final Set<TopicPartition> partitions = policy.tryCommit();
        logger.debug("Resume partitions: {}", partitions);
        consumer.resume(partitions);
    }
}
