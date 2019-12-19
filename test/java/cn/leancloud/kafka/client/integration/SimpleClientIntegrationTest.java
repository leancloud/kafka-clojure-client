package cn.leancloud.kafka.client.integration;

import cn.leancloud.kafka.client.consumer.LcKafkaConsumer;
import cn.leancloud.kafka.client.consumer.LcKafkaConsumerBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class SimpleClientIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleClientIntegrationTest.class);
    private String topic;
    private Producer<Integer, String> producer;

    public SimpleClientIntegrationTest() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        this.producer = new KafkaProducer<>(configs, new IntegerSerializer(), new StringSerializer());
        ;
        this.topic = "Testing";
    }

    void run() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
        configs.put("group.id", "2614911922612339122");
        configs.put("max.poll.records", 2);
        configs.put("max.poll.interval.ms", "5000");
        configs.put("enable.auto.commit", "true");
        configs.put("key.deserializer", IntegerDeserializer.class.getName());
        configs.put("value.deserializer", StringDeserializer.class.getName());

        LongAdder adder = new LongAdder();


        LcKafkaConsumer<Integer, String> client = LcKafkaConsumerBuilder.newBuilder(configs,
                (topic, value) -> {
                    logger.info("receive msg from {} with value: {}", topic, value);
                    adder.increment();
                })
                .buildAuto();

        client.subscribe(Collections.singletonList(topic));


        TestingProducer producer = new TestingProducer(topic, Duration.ofMillis(100), 4, Duration.ofSeconds(10));

        try {
            Thread.sleep(10000);
        } catch (Exception ex) {

        }
        System.out.println(producer.sent());
        System.out.println(adder.sumThenReset());
        client.close();
    }


    public static void main(String[] args) throws Exception {
        SimpleClientIntegrationTest test = new SimpleClientIntegrationTest();
        test.run();
    }
}
