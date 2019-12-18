package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;

public final class SimpleClient<K, V> implements Closeable {
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
    private final Thread fetcherThread;
    private final Fetcher<K, V> fetcher;
    private final CommitPolicy<K, V> policy;
    private volatile State state;

    private SimpleClient(Map<String, Object> consumerConfigs,
                         long pollTimeout,
                         MsgHandler<V> handler) {
        this.state = State.INIT;
        this.consumer = new KafkaConsumer<>(consumerConfigs);

        final ExecutorCompletionService<ConsumerRecord<K, V>> service =
                new ExecutorCompletionService<>(ImmediateExecutor.INSTANCE);
        this.policy = AutoCommitPolicy.getInstance();
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
