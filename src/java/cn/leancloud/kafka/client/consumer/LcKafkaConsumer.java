package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class LcKafkaConsumer<K, V> implements Closeable {
    enum State {
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
    private final Thread fetcherThread;
    private final Fetcher<K, V> fetcher;
    private final ExecutorService workerPool;
    private final CommitPolicy<K, V> policy;
    private final boolean shutdownWorkerPoolOnStop;
    private volatile State state;

    LcKafkaConsumer(LcKafkaConsumerBuilder<K, V> builder) {
        this.state = State.INIT;
        this.consumer = builder.getConsumer();
        this.workerPool = builder.getWorkerPool();
        this.shutdownWorkerPoolOnStop = builder.isShutdownWorkerPoolOnStop();
        this.policy = builder.getPolicy();
        this.fetcher = new Fetcher<>(
                consumer,
                builder.getPollTimeout(),
                builder.getMessageHandler(),
                builder.getWorkerPool(),
                policy);
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
