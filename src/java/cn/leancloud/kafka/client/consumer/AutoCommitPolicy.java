package cn.leancloud.kafka.client.consumer;

public final class AutoCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private static final AutoCommitPolicy INSTANCE = new AutoCommitPolicy();

    @SuppressWarnings("unchecked")
    public static <K, V> AutoCommitPolicy<K, V> getInstance() {
        return (AutoCommitPolicy<K, V>) INSTANCE;
    }

}
