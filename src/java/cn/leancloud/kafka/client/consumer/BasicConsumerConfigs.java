package cn.leancloud.kafka.client.consumer;

import javax.annotation.Nullable;

enum BasicConsumerConfigs implements KafkaConfigsChecker {
    AUTO_OFFSET_RESET("auto.offset.reset");

    private String config;
    @Nullable
    private String expectedValue;
    private boolean required;

    BasicConsumerConfigs(String config) {
        this.config = config;
        this.expectedValue = null;
        this.required = true;
    }

    BasicConsumerConfigs(String config, @Nullable String expectedValue, boolean required) {
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
}
