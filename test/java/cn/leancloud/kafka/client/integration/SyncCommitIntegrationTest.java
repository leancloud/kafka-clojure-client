package cn.leancloud.kafka.client.integration;

import cn.leancloud.kafka.client.consumer.LcKafkaConsumer;
import cn.leancloud.kafka.client.consumer.LcKafkaConsumerBuilder;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class SyncCommitIntegrationTest implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SyncCommitIntegrationTest.class);

    public static void main(String[] args) throws Exception {
        try (SyncCommitIntegrationTest test = new SyncCommitIntegrationTest()) {
            test.runSingleConsumerTest();

            test.runTwoConsumersInSameGroupTest();

            test.runJoinGroupTest();
        }
    }

    private TestingProducer producer = new TestingProducer(Duration.ofMillis(100), 4);
    private LongAdder receiveRecordsCounter = new LongAdder();
    private String topic;

    public SyncCommitIntegrationTest() {
        this.topic = "Testing";
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    private void runSingleConsumerTest() throws Exception {
        final LcKafkaConsumer<Integer, String> client = createConsumer("consumer");
        client.subscribe(Collections.singletonList(topic));
        final int totalSent = producer.startFixedDurationTest(topic, Duration.ofSeconds(9)).get();

        try {
            await().atMost(10, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> receiveRecordsCounter.sum() >= totalSent);

            logger.error("Integration test finished. sent: {}, received: {}", totalSent, receiveRecordsCounter.sum());
        } catch (Exception ex) {
            logger.error("Integration test got unexpected exception. sent: {}, received: {}", totalSent, receiveRecordsCounter.sum(), ex);
        } finally {
            receiveRecordsCounter.reset();
            client.close();
        }
    }

    private void runTwoConsumersInSameGroupTest() throws Exception {
        final LcKafkaConsumer<Integer, String> client = createConsumer("consumer1");
        final LcKafkaConsumer<Integer, String> client2 = createConsumer("consumer2");
        client.subscribe(Collections.singletonList(topic));
        client2.subscribe(Collections.singletonList(topic));
        final int totalSent = producer.startFixedDurationTest(topic, Duration.ofSeconds(9)).get();

        try {
            await().atMost(10, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> receiveRecordsCounter.sum() >= totalSent);

            logger.error("Integration test finished. sent: {}, received: {}", totalSent, receiveRecordsCounter.sum());
        } catch (Exception ex) {
            logger.error("Integration test got unexpected exception. sent: {}, received: {}", totalSent, receiveRecordsCounter.sum(), ex);
        } finally {
            receiveRecordsCounter.reset();
            client.close();
            client2.close();
        }
    }

    private void runJoinGroupTest() throws Exception {
        final AtomicBoolean stopProducer = new AtomicBoolean();
        final CompletableFuture<Integer> producerSentCountFuture = producer.startNonStopTest(topic, stopProducer);

        List<LcKafkaConsumer<Integer, String>> consumers = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            final LcKafkaConsumer<Integer, String> consumer = createConsumer("consumer" + i);
            consumer.subscribe(Collections.singletonList(topic));
            consumers.add(consumer);
            Thread.sleep(1000);
        }

        for (LcKafkaConsumer<Integer, String> consumer : consumers) {
            consumer.close();
            Thread.sleep(1000);
        }

        stopProducer.set(true);
        int totalSent = producerSentCountFuture.get();

        try {
            await().atMost(10, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> receiveRecordsCounter.sum() >= totalSent);

            logger.error("Integration test finished. sent: {}, received: {}", totalSent, receiveRecordsCounter.sum());
        } catch (Exception ex) {
            logger.error("Integration test got unexpected exception. sent: {}, received: {}", totalSent, receiveRecordsCounter.sum(), ex);
        } finally {
            receiveRecordsCounter.reset();
        }
    }

    private LcKafkaConsumer<Integer, String> createConsumer(String consumerName) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
        configs.put("group.id", "2614911922612339122");
        configs.put("max.poll.records", 2);
        configs.put("max.poll.interval.ms", "5000");

        return LcKafkaConsumerBuilder.newBuilder(
                configs,
                (record) -> {
                    logger.info("{} receive msg from {} with value: {}", consumerName, topic, record.value());
                    receiveRecordsCounter.increment();
                },
                new IntegerDeserializer(),
                new StringDeserializer())
                .buildSync();
    }
}
