package cn.leancloud.kafka.client.consumer;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

interface KafkaConfigs {
    String configName();

    @Nullable
    String expectedValue();

    boolean required();

    default Map<String, Object> set(Map<String, Object> configs, Object value) {
        final Map<String, Object> newConfigs = new HashMap<>(configs);

        newConfigs.put(configName(), value);
        return newConfigs;
    }

    default <T> T get(Map<String, Object> configs) {
        @SuppressWarnings("unchecked")
        T value = (T) configs.get(configName());
        return value;
    }
}
