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
import org.apache.pulsar.common.api.EncryptionContext;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
        PulsarSourceDescriptor descriptor = PulsarSourceDescriptor.of(TOPIC, startOffset, null, null, SERVICE_URL, ADMIN_URL);
        DoFn.ProcessContinuation result = dofnInstance.processElement(
                descriptor, tracker,null, (DoFn.OutputReceiver) receiver);
        assertEquals(DoFn.ProcessContinuation.stop(), result);
        assertEquals(3, receiver.getOutputs().size());
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
                mockMessages.add(new MockMessage(topic, timestamp, Long.valueOf(i), Long.valueOf(i), i));
            }
            currentMsg = 0;
        }

        public void emptyMockRecords() {
            this.mockMessages.clear();
        }

        public long getStartTimestamp() {
            System.out.println(mockMessages.get(0).getPublishTime());
            return mockMessages.get(0).getPublishTime();
        }

        public long getEndTimestamp() {
            return mockMessages.get(mockMessages.size()-1).getPublishTime();
        }

        @Override
        public String getTopic() {
            return this.topic;
        }

        @Override
        public Message<byte[]> readNext() throws PulsarClientException {
            if(currentMsg < mockMessages.size()) {
                currentMsg++;
            }
            if(currentMsg == 0) return null;
            return mockMessages.get(currentMsg);
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
        public void seek(Function<String, Object> function) throws PulsarClientException {

        }

        @Override
        public void close() throws IOException {

        }
    }

    private static class MockMessage implements Message<byte[]> {

        private String topic;
        private long ledgerId = 1L;
        private long entryId = 1L;
        private int partitionIndex = 1;
        private long timestamp;

        public MockMessage (String topic, long timestamp, long ledgerId, long entryId, int partitionIndex) {
            this.topic = topic;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.partitionIndex = partitionIndex;
            this.timestamp = timestamp;
        }

        public MockMessage(String topic) {
            this.topic = topic;
        }

        @Override
        public Map<String, String> getProperties() {
            return null;
        }

        @Override
        public boolean hasProperty(String name) {
            return false;
        }

        @Override
        public String getProperty(String name) {
            return null;
        }

        @Override
        public byte[] getData() {
            return new byte[0];
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public byte[] getValue() {
            return null;
        }

        @Override
        public MessageId getMessageId() {
            return DefaultImplementation.newMessageId(this.ledgerId,this.entryId,this.partitionIndex);
        }

        @Override
        public long getPublishTime() { return timestamp;
        }

        @Override
        public long getEventTime() {
            return 0;
        }

        @Override
        public long getSequenceId() {
            return 0;
        }

        @Override
        public String getProducerName() {
            return null;
        }

        @Override
        public boolean hasKey() {
            return false;
        }

        @Override
        public String getKey() {
            return null;
        }

        @Override
        public boolean hasBase64EncodedKey() {
            return false;
        }

        @Override
        public byte[] getKeyBytes() {
            return new byte[0];
        }

        @Override
        public boolean hasOrderingKey() {
            return false;
        }

        @Override
        public byte[] getOrderingKey() {
            return new byte[0];
        }

        @Override
        public String getTopicName() {
            return topic;
        }

        @Override
        public Optional<EncryptionContext> getEncryptionCtx() {
            return Optional.empty();
        }

        @Override
        public int getRedeliveryCount() {
            return 0;
        }

        @Override
        public byte[] getSchemaVersion() {
            return new byte[0];
        }

        @Override
        public boolean isReplicated() {
            return false;
        }

        @Override
        public String getReplicatedFrom() {
            return null;
        }

        @Override
        public void release() {

        }
    }
}
