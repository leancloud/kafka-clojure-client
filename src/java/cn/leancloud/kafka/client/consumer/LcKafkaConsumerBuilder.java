package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static java.util.Objects.requireNonNull;

public final class LcKafkaConsumerBuilder<K, V> {
    private static final ThreadFactory threadFactory = new NamedThreadFactory("lc-kafka-consumer-task-worker-pool");

    public static LcKafkaConsumerBuilder<Object, Object> newBuilder(Map<String, Object> configs,
                                                                    MessageHandler<Object> messageHandler) {
        return new LcKafkaConsumerBuilder<>(configs, messageHandler);
    }

    public static LcKafkaConsumerBuilder<?, ?> newBuilder(Map<String, Object> configs,
                                                          MessageHandler<Object> messageHandler,
                                                          Deserializer<?> keyDeserializer,
                                                          Deserializer<Object> valueDeserializer) {
        return new LcKafkaConsumerBuilder<>(configs, messageHandler, keyDeserializer, valueDeserializer);
    }

    /**
     * Ensures that the argument expression is true.
     */
    private static void requireArgument(boolean expression, String template, Object... args) {
        if (!expression) {
            throw new IllegalArgumentException(String.format(template, args));
        }
    }

    private long pollTimeout = 100;
    private int maxConsecutiveAsyncCommits = 10;
    private Map<String, Object> configs;
    private MessageHandler<V> messageHandler;
    @Nullable
    private Consumer<K, V> consumer;
    @Nullable
    private Deserializer<K> keyDeserializer;
    @Nullable
    private Deserializer<V> valueDeserializer;
    @Nullable
    private CommitPolicy<K, V> policy;
    @Nullable
    private ExecutorService workerPool;
    @Nullable
    private ExecutorCompletionService<ConsumerRecord<K, V>> completionWorkerService;
    private boolean shutdownWorkerPoolOnStop;

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs,
                                   MessageHandler<V> messageHandler) {
        requireNonNull(kafkaConsumerConfigs, "kafkaConsumerConfigs");
        requireNonNull(messageHandler, "messageHandler");
        this.configs = kafkaConsumerConfigs;
        this.messageHandler = messageHandler;
    }

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs,
                                   MessageHandler<V> messageHandler,
                                   Deserializer<K> keyDeserializer,
                                   Deserializer<V> valueDeserializer) {
        requireNonNull(kafkaConsumerConfigs, "kafkaConsumerConfigs");
        requireNonNull(messageHandler, "messageHandler");
        requireNonNull(keyDeserializer, "keyDeserializer");
        requireNonNull(valueDeserializer, "valueDeserializer");

        this.configs = kafkaConsumerConfigs;
        this.messageHandler = messageHandler;
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
        this.messageHandler = msgHandler;
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
        consumer = buildConsumer(true);
        policy = AutoCommitPolicy.getInstance();
        completionWorkerService = new ExecutorCompletionService<>(ImmediateExecutor.INSTANCE);
        shutdownWorkerPoolOnStop = false;
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildSync() {
        consumer = buildConsumer(false);
        policy = new SyncCommitPolicy<>(consumer);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialSync() {
        consumer = buildConsumer(false);
        policy = new PartialSyncCommitPolicy<>(consumer);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildAsync() {
        consumer = buildConsumer(false);
        policy = new AsyncCommitPolicy<>(consumer, maxConsecutiveAsyncCommits);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialAsync() {
        consumer = buildConsumer(false);
        policy = new PartialAsyncCommitPolicy<>(consumer, maxConsecutiveAsyncCommits);
        return doBuild();
    }

    Consumer<K, V> getConsumer() {
        assert consumer != null;
        return consumer;
    }

    MessageHandler<V> getMessageHandler() {
        return messageHandler;
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
        assert policy != null;
        return policy;
    }

    private Consumer<K, V> buildConsumer(boolean autoCommit) {
        checkConfigs(BasicConsumerConfigs.values());
        configs.put("enable.auto.commit", Boolean.toString(autoCommit));
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
