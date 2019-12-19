package cn.leancloud.kafka.client.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SafetyNetMessageHandler<V> implements MessageHandler<V> {
    private static final Logger logger = LoggerFactory.getLogger(SafetyNetMessageHandler.class);

    private final MessageHandler<V> innerHandler;
    private final TriConsumer<String, ? super V, Throwable> errorConsumer;

    public SafetyNetMessageHandler(MessageHandler<V> innerHandler) {
        this(innerHandler, (topic, value, throwable) -> logger.error("Handle message " + value + " from topic: " + topic + " failed."));
    }

    public SafetyNetMessageHandler(MessageHandler<V> innerHandler, TriConsumer<String, ? super V, Throwable> errorConsumer) {
        this.innerHandler = innerHandler;
        this.errorConsumer = errorConsumer;
    }

    @Override
    public void handleMessage(String topic, V value) {
        try {
            innerHandler.handleMessage(topic, value);
        } catch (Exception ex) {
            errorConsumer.accept(topic, value, ex);
        }
    }
}
