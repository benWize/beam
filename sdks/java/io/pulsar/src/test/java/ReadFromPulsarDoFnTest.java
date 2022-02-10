import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.pulsar.PulsarIO;
import org.apache.beam.sdk.io.pulsar.PulsarMessage;
import org.apache.beam.sdk.io.pulsar.PulsarSourceDescriptor;
import org.apache.beam.sdk.io.pulsar.ReadFromPulsarDoFn;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class ReadFromPulsarDoFnTest {

    public static final String SERVICE_URL = "pulsar://localhost:6650";
    public static final String ADMIN_URL = "http://localhost:8080";
    public static final String TOPIC = "PULSARIO_READFROMPULSAR_TEST";
    public static final int numberOfMessages = 100;
    public static final MockPulsarReader mockReader = new MockPulsarReader(TOPIC, numberOfMessages);
    private MockPulsarClient mockPulsarClient = new MockPulsarClient(mockReader);
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
        dofnInstance.setClient(this.mockPulsarClient);
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

}
