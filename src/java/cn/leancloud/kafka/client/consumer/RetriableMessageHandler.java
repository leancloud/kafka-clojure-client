package cn.leancloud.kafka.client.consumer;

public final class RetriableMessageHandler<V> implements MessageHandler<V> {
    private final int maxRetryTimes;
    private final MessageHandler<V> innerHandler;

    public RetriableMessageHandler(MessageHandler<V> innerHandler, int maxRetryTimes) {
        if (maxRetryTimes <= 0) {
            throw new IllegalArgumentException("maxRetryTimes: " + maxRetryTimes + " (expect > 0)");
        }

        this.maxRetryTimes = maxRetryTimes;
        this.innerHandler = innerHandler;
    }

    @Override
    public void handleMessage(String topic, V value) {
        Exception lastException = null;
        for (int retried = 0; retried < maxRetryTimes; ++retried) {
            try {
                innerHandler.handleMessage(topic, value);
                lastException = null;
                break;
            } catch (Exception ex) {
                lastException = ex;
            }
        }

        if (lastException != null) {
            throw new HandleMessageFailedException(lastException);
        }
    }
}
