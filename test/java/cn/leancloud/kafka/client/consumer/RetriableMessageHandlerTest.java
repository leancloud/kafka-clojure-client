package cn.leancloud.kafka.client.consumer;


import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class RetriableMessageHandlerTest {
    private MessageHandler<Object> innerHandler;

    @Before
    public void setUp() {
        innerHandler = mock(MessageHandler.class);
    }

    @Test
    public void testInvalidRetryTimes() {
        final int retryTimes = -1 * ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        assertThatThrownBy(() -> new RetriableMessageHandler<>(innerHandler, retryTimes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetryTimes");
    }

    @Test
    public void testRetry() {
        final Exception expectedEx = new RuntimeException();
        final int retryTimes = 10;
        final String topic = "topic";
        final Object msg = new Object();

        doThrow(expectedEx).when(innerHandler).handleMessage(topic, msg);

        MessageHandler<Object> handler = new RetriableMessageHandler<>(innerHandler, retryTimes);

        assertThatThrownBy(() -> handler.handleMessage(topic, msg))
                .isInstanceOf(HandleMessageFailedException.class)
                .hasCause(expectedEx);

        verify(innerHandler, times(retryTimes)).handleMessage(topic, msg);
    }

    @Test
    public void testNoRetry() {
        final int retryTimes = 10;
        final String topic = "topic";
        final Object msg = new Object();

        doNothing().when(innerHandler).handleMessage(topic, msg);

        MessageHandler<Object> handler = new RetriableMessageHandler<>(innerHandler, retryTimes);

        handler.handleMessage(topic, msg);

        verify(innerHandler, times(1)).handleMessage(topic, msg);
    }

    @Test
    public void testRetrySuccess() {
        final Exception expectedEx = new RuntimeException();
        final int retryTimes = 10;
        final String topic = "topic";
        final Object msg = new Object();

        doThrow(expectedEx).doNothing().when(innerHandler).handleMessage(topic, msg);

        MessageHandler<Object> handler = new RetriableMessageHandler<>(innerHandler, retryTimes);

        handler.handleMessage(topic, msg);

        verify(innerHandler, times(2)).handleMessage(topic, msg);
    }
}