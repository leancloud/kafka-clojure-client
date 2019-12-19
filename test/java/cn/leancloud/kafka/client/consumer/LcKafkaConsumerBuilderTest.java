package cn.leancloud.kafka.client.consumer;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class LcKafkaConsumerBuilderTest {

    private Map<String, Object> configs;
    private MessageHandler<Object> testingHandler;
    private Deserializer<Object> keyDeserializer;
    private Deserializer<Object> valueDeserializer;

    @Before
    public void setUp() throws Exception {
        configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("group.id", "2614911922612339122");

        testingHandler = mock(MessageHandler.class);
        keyDeserializer = mock(Deserializer.class);
        valueDeserializer = mock(Deserializer.class);
    }

    @Test
    public void testNullKafkaConfigs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(null, testingHandler))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("kafkaConsumerConfigs");

        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(null, testingHandler, keyDeserializer, valueDeserializer))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("kafkaConsumerConfigs");
    }

    @Test
    public void testNullMessageHandler() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messageHandler");

        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, null, keyDeserializer, valueDeserializer))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messageHandler");

        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .messageHandler(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messageHandler");
    }

    @Test
    public void testNullDeserializers() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, null, valueDeserializer))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("keyDeserializer");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("valueDeserializer");
    }

    @Test
    public void testNegativePollTimeoutMs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .pollTimeoutMs(-1 * ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pollTimeoutMs");
    }

    @Test
    public void testNegativeMaxConsecutiveAsyncCommits() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .maxConsecutiveAsyncCommits(-1 * ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConsecutiveAsyncCommits");
    }

    @Test
    public void testZeroMaxConsecutiveAsyncCommits() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .maxConsecutiveAsyncCommits(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConsecutiveAsyncCommits");
    }

    @Test
    public void testNullPollTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .pollTimeout(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("pollTimeout");
    }

    @Test
    public void testNullWorkerPool() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .workerPool(null, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("workerPool");
    }

    @Test
    public void testAutoConsumerWithoutMaxPollInterval() {
        configs.put("max.poll.records", "10");
        configs.put("auto.commit.interval.ms", "1000");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"max.poll.interval.ms\"");
    }

    @Test
    public void testAutoConsumerWithoutMaxPollRecords() {
        configs.put("max.poll.interval.ms", "1000");
        configs.put("auto.commit.interval.ms", "1000");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"max.poll.records\"");
    }

    @Test
    public void testAutoConsumerWithoutAutoCommitInterval() {
        configs.put("max.poll.records", "10");
        configs.put("max.poll.interval.ms", "1000");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"auto.commit.interval.ms\"");

    }
}