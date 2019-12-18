package cn.leancloud.kafka.client.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.LongAdder;

public class TestingProducer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TestingProducer.class);

    private LongAdder counter;
    private Duration sendInterval;
    private Instant end;
    private Producer<Integer, String> producer;
    private String topic;
    private CyclicBarrier barrier;
    private volatile boolean closed;

    public TestingProducer(String topic, Duration sendInterval, int concurrentThreadCount, Duration testingTime) throws Exception {
        this.topic = topic;
        this.counter = new LongAdder();
        this.sendInterval = sendInterval;
        this.end = Instant.now().plus(testingTime);

        final Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        this.producer = new KafkaProducer<>(configs, new IntegerSerializer(), new StringSerializer());
        this.barrier = new CyclicBarrier(concurrentThreadCount + 1);
        for (int i = 0; i < concurrentThreadCount; ++i) {
            final Thread t = new Thread(new ProducerWorker());
            t.start();
        }
        barrier.await();
    }

    public CompletableFuture<Void> startTest() {
        return null;
    }

    public long sent() {
        return counter.sumThenReset();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        try {
            barrier.await();
        } catch (Exception ex) {
            //
        }
    }

    private class ProducerWorker implements Runnable {
        @Override
        public void run() {
            try {
                final LongAdder counter = TestingProducer.this.counter;
                final String name = Thread.currentThread().getName();
                final Instant end = TestingProducer.this.end;
                final long intervalMs = TestingProducer.this.sendInterval.toMillis();
                barrier.await();
                int index = 0;
                while (!closed && Instant.now().isBefore(end)) {

                    final ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, index, name + "-" + index++);
                    producer.send(record, (metadata, exception) -> {
                    });

                    counter.increment();

                    Thread.sleep(intervalMs);
                }
                barrier.await();
            } catch (Exception ex) {
                logger.error("Producer worker got unexpected exception", ex);
            }
        }
    }
}
