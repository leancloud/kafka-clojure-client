package cn.leancloud.kafka.client.integration;

import cn.leancloud.kafka.client.consumer.LcKafkaConsumer;
import cn.leancloud.kafka.client.consumer.LcKafkaConsumerBuilder;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class SyncCommitIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(SyncCommitIntegrationTest.class);
    private String topic;

    public SyncCommitIntegrationTest() {
        this.topic = "Testing";
    }

    void run() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
        configs.put("group.id", "2614911922612339122");
        configs.put("max.poll.records", 2);
        configs.put("max.poll.interval.ms", "5000");
        configs.put("key.deserializer", IntegerDeserializer.class.getName());
        configs.put("value.deserializer", StringDeserializer.class.getName());

        LongAdder adder = new LongAdder();
        LcKafkaConsumer<Integer, String> client = LcKafkaConsumerBuilder.newBuilder(configs)
                .messageHandler((topic, value) -> {
                    logger.info("receive msg from {} with value: {}", topic, value);
                    adder.increment();
                })
                .buildSync();

        client.subscribe(Collections.singletonList(topic));


        TestingProducer producer = new TestingProducer(topic, Duration.ofMillis(100), 4, Duration.ofSeconds(9));

        try {
            Thread.sleep(10000);
        } catch (Exception ex) {

        }
        System.out.println(producer.sent());
        System.out.println(adder.sumThenReset());
    }


    public static void main(String[] args) throws Exception {
        SyncCommitIntegrationTest test = new SyncCommitIntegrationTest();
        test.run();
    }
}
