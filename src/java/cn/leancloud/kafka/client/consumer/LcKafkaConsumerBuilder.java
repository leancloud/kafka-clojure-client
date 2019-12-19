package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static java.util.Objects.requireNonNull;

public final class LcKafkaConsumerBuilder<K, V> {
    private static final ThreadFactory threadFactory = new NamedThreadFactory("lc-kafka-consumer-task-worker-pool");

    public static LcKafkaConsumerBuilder<Object, Object> newBuilder(Map<String, Object> configs) {
        return new LcKafkaConsumerBuilder<>(configs);
    }

    public static LcKafkaConsumerBuilder<?, ?> newBuilder(Map<String, Object> configs,
                                                          Deserializer<?> keyDeserializer,
                                                          Deserializer<?> valueDeserializer) {
        return new LcKafkaConsumerBuilder<>(configs, keyDeserializer, valueDeserializer);
    }

    /**
     * Ensures that the argument expression is true.
     */
    private static void requireArgument(boolean expression, String template, Object... args) {
        if (!expression) {
            throw new IllegalArgumentException(String.format(template, args));
        }
    }

    private Consumer<K, V> consumer;
    private Map<String, Object> configs;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private CommitPolicy<K, V> policy;
    private MessageHandler<V> handler;
    private ExecutorService workerPool;
    private ExecutorCompletionService<ConsumerRecord<K, V>> completionWorkerService;
    private boolean shutdownWorkerPoolOnStop;
    private long pollTimeout = 100;
    private int maxConsecutiveAsyncCommits = 10;

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs) {
        requireNonNull(kafkaConsumerConfigs, "kafkaConsumerConfigs");
        this.configs = kafkaConsumerConfigs;
    }

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs,
                                   Deserializer<K> keyDeserializer,
                                   Deserializer<V> valueDeserializer) {
        requireNonNull(kafkaConsumerConfigs, "kafkaConsumerConfigs");
        requireNonNull(keyDeserializer, "keyDeserializer");
        requireNonNull(valueDeserializer, "valueDeserializer");

        this.configs = kafkaConsumerConfigs;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public LcKafkaConsumerBuilder<K, V> pollTimeoutMs(long pollTimeoutMs) {
        requireArgument(pollTimeoutMs >= 0, "pollTimeoutMs: %s (expect >= 0)", pollTimeoutMs);
        this.pollTimeout = pollTimeoutMs;
        return this;
    }

    public LcKafkaConsumerBuilder<K, V> pollTimeout(Duration pollTimeout) {
        requireNonNull(pollTimeout, "pollTimeout");
        this.pollTimeout = pollTimeout.toMillis();
        return this;
    }

    public LcKafkaConsumerBuilder<K, V> maxConsecutiveAsyncCommits(int maxConsecutiveAsyncCommits) {
        requireArgument(maxConsecutiveAsyncCommits > 0,
                "maxConsecutiveAsyncCommits: %s (expect > 0)", maxConsecutiveAsyncCommits);
        this.maxConsecutiveAsyncCommits = maxConsecutiveAsyncCommits;
        return this;
    }

    public LcKafkaConsumerBuilder<K, V> messageHandler(MessageHandler<V> msgHandler) {
        requireNonNull(msgHandler, "msgHandler");
        this.handler = msgHandler;
        return this;
    }

    public LcKafkaConsumerBuilder<K, V> workerPool(ExecutorService workerPool, boolean shutdownOnStop) {
        requireNonNull(workerPool, "workerPool");
        this.workerPool = workerPool;
        this.shutdownWorkerPoolOnStop = shutdownOnStop;
        return this;
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildAuto() {
        checkConfigs(AutoCommitConsumerConfigs.values());
        configs.put("enable.auto.commit", "true");
        consumer = buildConsumer();
        policy = AutoCommitPolicy.getInstance();
        completionWorkerService = new ExecutorCompletionService<>(ImmediateExecutor.INSTANCE);
        shutdownWorkerPoolOnStop = false;
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildSync() {
        configs.put("enable.auto.commit", "false");
        consumer = buildConsumer();
        policy = new SyncCommitPolicy<>(consumer);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialSync() {
        configs.put("enable.auto.commit", "false");
        consumer = buildConsumer();
        policy = new PartialSyncCommitPolicy<>(consumer);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildAsync() {
        configs.put("enable.auto.commit", "false");
        consumer = buildConsumer();
        policy = new AsyncCommitPolicy<>(consumer, maxConsecutiveAsyncCommits);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialAsync() {
        configs.put("enable.auto.commit", "false");
        consumer = buildConsumer();
        policy = new PartialAsyncCommitPolicy<>(consumer, maxConsecutiveAsyncCommits);
        return doBuild();
    }

    Consumer<K, V> getConsumer() {
        return consumer;
    }

    MessageHandler<V> getMessageHandler() {
        return handler;
    }

    ExecutorService getWorkerPool() {
        if (workerPool == null) {
            workerPool = Executors.newCachedThreadPool(threadFactory);
            shutdownWorkerPoolOnStop = true;
        }
        return workerPool;
    }

    ExecutorCompletionService<ConsumerRecord<K, V>> getCompletionWorkerService() {
        if (completionWorkerService == null) {
            completionWorkerService = new ExecutorCompletionService<>(getWorkerPool());
        }
        return completionWorkerService;
    }

    boolean isShutdownWorkerPoolOnStop() {
        return shutdownWorkerPoolOnStop;
    }

    long getPollTimeout() {
        return pollTimeout;
    }

    CommitPolicy<K, V> getPolicy() {
        return policy;
    }

    private Consumer<K, V> buildConsumer() {
        checkConfigs(BasicConsumerConfigs.values());

        if (keyDeserializer != null) {
            assert valueDeserializer != null;
            return new KafkaConsumer<>(configs, keyDeserializer, valueDeserializer);
        }

        assert valueDeserializer == null;
        return new KafkaConsumer<>(configs);
    }

    private void checkConfigs(KafkaConfigsChecker[] checkers) {
        for (KafkaConfigsChecker check : checkers) {
            check.check(configs);
        }
    }

    private <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> doBuild() {
        @SuppressWarnings("unchecked")
        final LcKafkaConsumer<K1, V1> c = (LcKafkaConsumer<K1, V1>) new LcKafkaConsumer<>(this);
        return c;
    }
}
