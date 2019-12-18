package cn.leancloud.kafka.client.consumer;

import java.util.Collection;

abstract class AbstractLKafkaConsumer implements LKafkaConsumer {
    enum State {
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

    private volatile State state;

    public synchronized void subscribe(Collection<String> topics) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("empty topics");
        }

        if (subscribed() || closed()) {
            throw new IllegalStateException("client is in " + state + " state. expect: " + State.INIT);
        }

        doSubscribe(topics);

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

        doClose();
    }

    abstract void doSubscribe(Collection<String> topics);

    abstract void doClose();

    private boolean subscribed() {
        return state.code() > State.INIT.code();
    }

    private boolean closed() {
        return state == State.CLOSED;
    }

}
