package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

public final class ComplexLConsumer<K, V> extends AbstractLKafkaConsumer {
    private static final ThreadFactory threadFactory = new NamedThreadFactory("back-pressure-task-worker-pool");

    private final Consumer<K, V> consumer;
    private final ExecutorService workerPool;
    private final Thread fetcherThread;
    private final Fetcher<K, V> fetcher;
    private final CommitPolicy<K, V> policy;
    private final boolean shutdownWorkerPoolOnStop;

    public ComplexLConsumer(Map<String, Object> consumerConfigs,
                            long pollTimeout,
                            MsgHandler<V> handler) {
        this(consumerConfigs, pollTimeout, handler, null, true);
    }

    public ComplexLConsumer(Map<String, Object> consumerConfigs,
                            long pollTimeout,
                            MsgHandler<V> handler,
                            ExecutorService workerPool) {
        this(consumerConfigs, pollTimeout, handler, workerPool, false);
    }

    private ComplexLConsumer(Map<String, Object> consumerConfigs,
                             long pollTimeout,
                             MsgHandler<V> handler,
                             ExecutorService workerPool,
                             boolean shutdownWorkerPoolOnStop) {
        this.consumer = new KafkaConsumer<>(consumerConfigs);
        if (workerPool == null) {
            workerPool = Executors.newCachedThreadPool(threadFactory);
        }

        this.workerPool = workerPool;
        this.shutdownWorkerPoolOnStop = shutdownWorkerPoolOnStop;

        final ExecutorCompletionService<ConsumerRecord<K, V>> service = new ExecutorCompletionService<>(workerPool);
        this.policy = new SyncCommitPolicy<>(consumer);
        this.fetcher = new Fetcher<>(consumer, pollTimeout, handler, service, policy);
        this.fetcherThread = new Thread(fetcher);
    }

    @Override
    public void doSubscribe(Collection<String> topics) {
        consumer.subscribe(topics, new RebalanceListener<>(fetcher, policy));

        final String firstTopic = topics.iterator().next();
        fetcherThread.setName("kafka-fetcher-for-" + firstTopic + (topics.size() > 1 ? "..." : ""));
        fetcherThread.start();
    }

    @Override
    public void doClose() {
        fetcher.close();
        try {
            fetcherThread.join();
            consumer.close();
            if (shutdownWorkerPoolOnStop) {
                workerPool.shutdown();
                workerPool.awaitTermination(1, TimeUnit.DAYS);
            }
        } catch (InterruptedException ex) {
            // ignore
        }
    }
}
