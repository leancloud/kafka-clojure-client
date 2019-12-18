package cn.leancloud.kafka.client.consumer;

import java.io.Closeable;
import java.util.Collection;

public interface LKafkaConsumer extends Closeable {
    void subscribe(Collection<String> topics);
}
