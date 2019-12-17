package cn.leancloud.kafka.client.integration;

import cn.leancloud.kafka.client.consumer.BackPressureClient;
import cn.leancloud.kafka.client.consumer.NamedThreadFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class SyncCommitIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(SyncCommitIntegrationTest.class);
    private String topic;
    private Producer<Integer, String> producer;

    public SyncCommitIntegrationTest() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        this.producer = new KafkaProducer<>(configs, new IntegerSerializer(), new StringSerializer());
        ;
        this.topic = "Testing";
    }

    void run() {
        for (int i = 0; i < 1000; ++i) {
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, i, "" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    logger.info("producer callback {} {}", metadata, exception);
                }
            });
        }

        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
        configs.put("group.id", "2614911922612339122");

        Consumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(
                configs,
                new IntegerDeserializer(),
                new StringDeserializer());

        BackPressureClient<Integer, String> client = new BackPressureClient<>(
                consumer,
                Executors.newCachedThreadPool(new NamedThreadFactory("consumer", true)),
                100,
                (topic, value) -> {
                    logger.info("receive msg from {} with value: {}", topic, value);
                }
        );

        client.subscribe(Collections.singletonList(topic));
        try {
            Thread.sleep(100000);
        } catch (Exception ex) {

        }
    }


    public static void main(String[] args) {
        SyncCommitIntegrationTest test = new SyncCommitIntegrationTest();
        test.run();
    }
}
