package cn.leancloud.kafka.client.consumer;

import javax.annotation.Nullable;
import java.util.Map;

enum BasicConsumerConfigs implements KafkaConfigsChecker {
    AUTO_OFFSET_RESET("auto.offset.reset"),
    ENABLE_AUTO_COMMIT("enable.auto.commit", false);

    private String config;
    @Nullable
    private String expectedValue;
    private boolean required;

    BasicConsumerConfigs(String config) {
        this.config = config;
        this.expectedValue = null;
        this.required = true;
    }

    BasicConsumerConfigs(String config, boolean required) {
        this.config = config;
        this.expectedValue = null;
        this.required = required;
    }

    BasicConsumerConfigs(String config, boolean required, @Nullable String expectedValue) {
        this.config = config;
        this.expectedValue = expectedValue;
        this.required = required;
    }

    @Override
    public String configName() {
        return config;
    }

    @Override
    public String expectedValue() {
        return expectedValue;
    }

    @Override
    public boolean required() {
        return required;
    }

    public void set(Map<String, Object> configs, Object value) {
        configs.put(configName(), value);
    }

    public <T> T get(Map<String, Object> configs) {
        @SuppressWarnings("unchecked")
        T value = (T) configs.get(configName());
        return value;
    }
}
