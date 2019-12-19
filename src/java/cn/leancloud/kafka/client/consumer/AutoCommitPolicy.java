package cn.leancloud.kafka.client.consumer;

final class AutoCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private static final AutoCommitPolicy INSTANCE = new AutoCommitPolicy();

    @SuppressWarnings("unchecked")
    static <K, V> AutoCommitPolicy<K, V> getInstance() {
        return (AutoCommitPolicy<K, V>) INSTANCE;
    }

}
