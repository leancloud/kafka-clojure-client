package cn.leancloud.kafka.client.consumer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

final class ImmediateExecutorService extends AbstractExecutorService {
    static final ImmediateExecutorService INSTANCE = new ImmediateExecutorService();

    private boolean terminated;

    private ImmediateExecutorService() {
    }

    @Override
    public void shutdown() {
        terminated = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        terminated = true;
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return terminated;
    }

    @Override
    public boolean isTerminated() {
        return terminated;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (terminated) {
            return true;
        } else {
            Thread.sleep(unit.toMillis(timeout));
            return false;
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException("command");
        } else {
            command.run();
        }
    }
}
