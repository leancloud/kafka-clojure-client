package cn.leancloud.kafka.client.consumer;

import java.util.concurrent.Executor;

final class ImmediateExecutor implements Executor {
    static final ImmediateExecutor INSTANCE = new ImmediateExecutor();

    private ImmediateExecutor() {
    }

    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException("command");
        } else {
            command.run();
        }
    }
}
