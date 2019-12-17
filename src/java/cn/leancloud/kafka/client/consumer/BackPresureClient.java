package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class BackPresureClient<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(BackPresureClient.class.getSimpleName());

    private final Consumer<K, V> consumer;
    private final ExecutorService workerPool;
    private final Thread fetcherThread;
    private final ExecutorCompletionService<Result> service;
    private final Set<TopicPartition> pausedPartitions;
    private final MsgHandler<V> handler;

    public BackPresureClient(Consumer<K, V> consumer,
                             ExecutorService workerPool,
                             long pollTimeout,
                             MsgHandler<V> handler) {
        this.consumer = consumer;
        this.workerPool = workerPool;
        this.fetcherThread = new Thread(new Fetcher(consumer, pollTimeout));
        this.service = new ExecutorCompletionService<>(workerPool);
        this.pausedPartitions = new HashSet<>();
        this.handler = handler;
    }


    public void subscribe(Collection<String> topics, boolean consumeFromLargest) {
        consumer.subscribe(topics, new RebalanceListener(pausedPartitions));
        if (consumeFromLargest) {
            consumer.seekToEnd(Collections.emptyList());
        }

        fetcherThread.run();
    }

    private class Fetcher implements Runnable {
        private long pollTimeout;
        private Consumer<K, V> consumer;

        Fetcher(Consumer<K, V> consumer, long pollTimeout) {
            this.pollTimeout = pollTimeout;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            int pendingCount = 0;
            while (true) {
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
            }
        }

        void pauseAssignedPartitions() {
            consumer.pause(consumer.assignment());
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
