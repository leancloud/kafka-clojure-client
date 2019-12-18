package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

public class BackPressureClient<K, V> implements Closeable {
    private static final ThreadFactory threadFactory = new NamedThreadFactory("back-pressure-task-worker-pool");

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
    private final Fetcher<K, V> fetcher;
    private final CommitPolicy<K, V> policy;
    private final boolean shutdownWorkerPoolOnStop;
    private volatile State state;

    public BackPressureClient(Map<String, Object> consumerConfigs,
                              long pollTimeout,
                              MsgHandler<V> handler) {
        this(consumerConfigs, pollTimeout, handler, null, true);
    }

    public BackPressureClient(Map<String, Object> consumerConfigs,
                              long pollTimeout,
                              MsgHandler<V> handler,
                              ExecutorService workerPool) {
        this(consumerConfigs, pollTimeout, handler, workerPool, false);
    }

    private BackPressureClient(Map<String, Object> consumerConfigs,
                               long pollTimeout,
                               MsgHandler<V> handler,
                               ExecutorService workerPool,
                               boolean shutdownWorkerPoolOnStop) {
        this.state = State.INIT;
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

    public synchronized void subscribe(Collection<String> topics) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("empty topics");
        }

        if (subscribed() || closed()) {
            throw new IllegalStateException("client is in " + state + " state. expect: " + State.INIT);
        }

        consumer.subscribe(topics, new RebalanceListener<>(fetcher, policy));

        final String firstTopic = topics.iterator().next();
        fetcherThread.setName("kafka-fetcher-for-" + firstTopic + (topics.size() > 1 ? "..." : ""));
        fetcherThread.start();
        state = State.SUBSCRIBED;
    }

    @Override
    public void close() {
        if (closed()) {
            return;
        }

        synchronized (this) {
            if (closed()) {
                return;
            }

            state = State.CLOSED;
        }

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

    private boolean subscribed() {
        return state.code() > State.INIT.code();
    }

    private boolean closed() {
        return state == State.CLOSED;
    }
}
