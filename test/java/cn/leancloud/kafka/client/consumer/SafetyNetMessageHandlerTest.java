package cn.leancloud.kafka.client.consumer;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class SafetyNetMessageHandlerTest {
    private MessageHandler<Object> innerHandler;

    @Before
    public void setUp() {
        innerHandler = mock(MessageHandler.class);
    }

    @Test
    public void testCustomErrorHandler() {
        final Exception expectedEx = new RuntimeException();
        final String topic = "topic";
        final Object msg = new Object();
        final TriConsumer<String, Object, Throwable> errorConsumer = mock(TriConsumer.class);

        doThrow(expectedEx).when(innerHandler).handleMessage(topic, msg);

        MessageHandler<Object> handler = new SafetyNetMessageHandler<>(innerHandler, errorConsumer);

        handler.handleMessage(topic, msg);

        verify(innerHandler, times(1)).handleMessage(topic, msg);
        verify(errorConsumer,times(1)).accept(topic, msg, expectedEx);
    }

    @Test
    public void testDefaultErrorHandler() {
        final Exception expectedEx = new RuntimeException();
        final String topic = "topic";
        final Object msg = new Object();

        doThrow(expectedEx).when(innerHandler).handleMessage(topic, msg);

        MessageHandler<Object> handler = new SafetyNetMessageHandler<>(innerHandler);

        handler.handleMessage(topic, msg);

        verify(innerHandler, times(1)).handleMessage(topic, msg);
    }


    @Test
    public void testHandleSuccess() {
        final String topic = "topic";
        final Object msg = new Object();
        final TriConsumer<String, Object, Throwable> errorConsumer = mock(TriConsumer.class);

        doNothing().when(innerHandler).handleMessage(topic, msg);

        MessageHandler<Object> handler = new SafetyNetMessageHandler<>(innerHandler, errorConsumer);

        handler.handleMessage(topic, msg);

        verify(innerHandler, times(1)).handleMessage(topic, msg);
        verify(errorConsumer,never()).accept(anyString(), any(), any());
    }
}