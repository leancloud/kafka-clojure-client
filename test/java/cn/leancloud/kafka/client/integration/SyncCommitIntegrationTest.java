package cn.leancloud.kafka.client.integration;

import cn.leancloud.kafka.client.consumer.BackPressureClient;
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
        configs.put("key.deserializer", IntegerDeserializer.class.getName());
        configs.put("value.deserializer", StringDeserializer.class.getName());

        BackPressureClient<Integer, String> client = new BackPressureClient<>(
                configs,
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
