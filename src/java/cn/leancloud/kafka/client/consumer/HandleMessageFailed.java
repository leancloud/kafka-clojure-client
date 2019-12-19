package cn.leancloud.kafka.client.consumer;

public class HandleMessageFailed extends RuntimeException {
    public HandleMessageFailed() {
        super();
    }

    public HandleMessageFailed(String message) {
        super(message);
    }

    public HandleMessageFailed(Throwable throwable) {
        super(throwable);
    }

    public HandleMessageFailed(String message, Throwable throwable) {
        super(message, throwable);
    }
}

