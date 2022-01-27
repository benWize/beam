import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.pulsar.PulsarIO;
import org.apache.beam.sdk.io.pulsar.PulsarMessage;
import org.apache.beam.sdk.io.pulsar.PulsarSourceDescriptor;
import org.apache.beam.sdk.io.pulsar.ReadFromPulsarDoFn;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@RunWith(JUnit4.class)
public class ReadFromPulsarDoFnTest {

    public static final String SERVICE_URL = "pulsar://localhost:6650";
    public static final String ADMIN_URL = "http://localhost:8080";
    public static final String TOPIC = "PULSARIO_READFROMPULSAR_TEST";
    public static final int numberOfMessages = 100;
    public static final SimpleMockPulsarReader mockReader = new SimpleMockPulsarReader(TOPIC, numberOfMessages);
    private final ReadFromPulsarDoFn dofnInstance = new ReadFromPulsarDoFn(readSourceDescriptor());

    private PulsarIO.Read readSourceDescriptor() {
        return PulsarIO.read()
                .withClientUrl(SERVICE_URL)
                .withTopic(TOPIC)
                .withAdminUrl(ADMIN_URL)
                .withPublishTime();
    }

    @Before
    public void setup() throws Exception {
        dofnInstance.setReader(mockReader);
        mockReader.reset();
    }

    @Test
    public void testInitialRestrictionWhenHasStartOffset() throws Exception {
        long expectedStartOffset = 0;
        OffsetRange result = dofnInstance.getInitialRestriction(PulsarSourceDescriptor.of(
                TOPIC, expectedStartOffset, null, null, SERVICE_URL, ADMIN_URL));
        assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
    }

    @Test
    public void testInitialRestrictionWithConsumerPosition() throws Exception {
        long expectedStartOffset = Instant.now().getMillis();
        OffsetRange result = dofnInstance.getInitialRestriction(PulsarSourceDescriptor.of(
                TOPIC, expectedStartOffset, null, null, SERVICE_URL, ADMIN_URL));
        assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
    }

    @Test
    public void testInitialRestrictionWithConsumerEndPosition() throws Exception {
        long startOffset = mockReader.getStartTimestamp();
        long endOffset = mockReader.getEndTimestamp();
        OffsetRange result = dofnInstance.getInitialRestriction(PulsarSourceDescriptor.of(
                TOPIC,
                startOffset,
                endOffset,
                null,
                SERVICE_URL,
                ADMIN_URL
        ));
        assertEquals(new OffsetRange(startOffset, endOffset), result);
    }

    @Test
    public void testProcessElement() throws Exception {
        MockOutputReceiver receiver = new MockOutputReceiver();
        long startOffset = mockReader.getStartTimestamp();
        long endOffset = mockReader.getEndTimestamp();
        OffsetRangeTracker tracker =
                new OffsetRangeTracker(new OffsetRange(startOffset, endOffset));
        PulsarSourceDescriptor descriptor = PulsarSourceDescriptor.of(
                TOPIC,
                startOffset,
                endOffset,
                null,
                SERVICE_URL,
                ADMIN_URL);
        DoFn.ProcessContinuation result = dofnInstance.processElement(
                descriptor, tracker,null, (DoFn.OutputReceiver) receiver);
        int expectedResultWithoutCountingLastOffset = numberOfMessages-1;
        assertEquals(DoFn.ProcessContinuation.stop(), result);
        assertEquals(expectedResultWithoutCountingLastOffset, receiver.getOutputs().size());
    }

    @Test
    public void testProcessElementWhenEndMessageIdIsDefined() throws Exception{
        MockOutputReceiver receiver = new MockOutputReceiver();
        OffsetRangeTracker tracker = new OffsetRangeTracker(
                new OffsetRange(0L, Long.MAX_VALUE)
        );
        MessageId endMessageId = DefaultImplementation.newMessageId(50L, 50L, 50);
        DoFn.ProcessContinuation result = dofnInstance.processElement(
                PulsarSourceDescriptor.of(TOPIC,
                        null,
                        null,
                        endMessageId,
                        SERVICE_URL,
                        ADMIN_URL),
                tracker,
                null,
                (DoFn.OutputReceiver) receiver);
        assertEquals(DoFn.ProcessContinuation.stop(), result);
        assertEquals(50, receiver.getOutputs().size());
    }

    @Test
    public void testProcessElementWithEmptyRecords() throws Exception {
        MockOutputReceiver receiver = new MockOutputReceiver();
        mockReader.emptyMockRecords();
        OffsetRangeTracker tracker = new OffsetRangeTracker(
                new OffsetRange(0L, Long.MAX_VALUE));
        DoFn.ProcessContinuation result =
                dofnInstance.processElement(
                        PulsarSourceDescriptor.of(
                                TOPIC,
                                null,
                                null,
                                null,
                                SERVICE_URL,
                                ADMIN_URL),
                        tracker,
                        null,
                        (DoFn.OutputReceiver) receiver);
        assertEquals(DoFn.ProcessContinuation.resume(), result);
        assertTrue(receiver.getOutputs().isEmpty());
    }

    @Test
    public void testProcessElementWhenHasReachedEndTopic() throws Exception {
        MockOutputReceiver receiver = new MockOutputReceiver();
        mockReader.setReachedEndOfTopic(true);
        OffsetRangeTracker tracker = new OffsetRangeTracker(
                new OffsetRange(0L, Long.MAX_VALUE)
        );
        DoFn.ProcessContinuation result = dofnInstance.processElement(
                PulsarSourceDescriptor.of(TOPIC,
                        null,
                        null,
                        null,
                        SERVICE_URL,
                        ADMIN_URL),
                tracker,
                null,
                (DoFn.OutputReceiver) receiver);
        assertEquals(DoFn.ProcessContinuation.stop(), result);
    }

    private static class MockOutputReceiver implements DoFn.OutputReceiver<PulsarMessage> {

        private final List<PulsarMessage> records = new ArrayList<>();

        @Override
        public void output(PulsarMessage output) {}

        @Override
        public void outputWithTimestamp(PulsarMessage output,
                                        @UnknownKeyFor @NonNull @Initialized Instant timestamp) {
            records.add(output);
        }

        public List<PulsarMessage> getOutputs() {
            return records;
        }
    }

    private static class SimpleMockPulsarReader implements Reader<byte[]> {

        private String topic;
        private List<MockMessage> mockMessages = new ArrayList<>();
        private int currentMsg;
        private long startTimestamp;
        private long endTimestamp;
        private boolean reachedEndOfTopic;

        public SimpleMockPulsarReader(String topic, int numberOfMessages) {
            this.setMock(topic, numberOfMessages);
        }

        public void setReachedEndOfTopic(boolean hasReachedEnd) {
            this.reachedEndOfTopic = hasReachedEnd;
        }

        public void setMock(String topic, int numberOfMessages) {
            this.topic = topic;
            for(int i=0; i<numberOfMessages; i++) {
                long timestamp = Instant.now().plus(Duration.standardSeconds(i)).getMillis();
                if(i==0) startTimestamp = timestamp;
                else if(i==99) endTimestamp = timestamp;
                mockMessages.add(new MockMessage(topic, timestamp, Long.valueOf(i), Long.valueOf(i), i));
            }
            currentMsg = 0;
        }

        public void reset() {
            this.reachedEndOfTopic = false;
            this.currentMsg = 0;
            emptyMockRecords();
            setMock(TOPIC, numberOfMessages);
        }

        public void emptyMockRecords() {
            this.mockMessages.clear();
        }

        public long getStartTimestamp() {
            return this.startTimestamp;
        }

        public long getEndTimestamp() {
            return this.endTimestamp;
        }

        @Override
        public String getTopic() {
            return this.topic;
        }

        @Override
        public Message<byte[]> readNext() throws PulsarClientException {
            if(currentMsg == 0 && mockMessages.isEmpty()) return null;

            Message<byte[]> msg = mockMessages.get(currentMsg);
            if(currentMsg <= mockMessages.size()-1) {
                currentMsg++;
            }
            return msg;
        }

        @Override
        public Message<byte[]> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
            return null;
        }

        @Override
        public CompletableFuture<Message<byte[]>> readNextAsync() {
            return null;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return null;
        }

        @Override
        public boolean hasReachedEndOfTopic() {
            return this.reachedEndOfTopic;
        }

        @Override
        public boolean hasMessageAvailable() throws PulsarClientException {
            return false;
        }

        @Override
        public CompletableFuture<Boolean> hasMessageAvailableAsync() {
            return null;
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public void seek(MessageId messageId) throws PulsarClientException {

        }

        @Override
        public void seek(long timestamp) throws PulsarClientException {
            for(int i=0; i< mockMessages.size(); i++) {
                if(timestamp == mockMessages.get(i).getPublishTime()) {
                    currentMsg = i;
                    break;
                }
            }
        }

        @Override
        public CompletableFuture<Void> seekAsync(MessageId messageId) {
            return null;
        }

        @Override
        public CompletableFuture<Void> seekAsync(long timestamp) {
            return null;
        }

        @Override
        public CompletableFuture<Void> seekAsync(Function<String, Object> function) {
            return null;
        }

        @Override
        public void seek(Function<String, Object> function) throws PulsarClientException { }

        @Override
        public void close() throws IOException { }
    }

}
