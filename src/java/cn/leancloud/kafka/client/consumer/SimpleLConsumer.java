package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;

public final class SimpleLConsumer<K, V> extends AbstractLKafkaConsumer {
    private final Consumer<K, V> consumer;
    private final Thread fetcherThread;
    private final Fetcher<K, V> fetcher;
    private final CommitPolicy<K, V> policy;

    public SimpleLConsumer(Map<String, Object> consumerConfigs,
                           long pollTimeout,
                           MsgHandler<V> handler) {
        this.consumer = new KafkaConsumer<>(consumerConfigs);

        final ExecutorCompletionService<ConsumerRecord<K, V>> service =
                new ExecutorCompletionService<>(ImmediateExecutor.INSTANCE);
        this.policy = AutoCommitPolicy.getInstance();
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
        } catch (InterruptedException ex) {
            // ignore
        }
    }
}
