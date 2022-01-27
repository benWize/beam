package org.apache.beam.sdk.io.pulsar;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

@DoFn.UnboundedPerElement
@SuppressWarnings("rawtypes")
public class ReadFromPulsarDoFn extends DoFn<PulsarSourceDescriptor, PulsarMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFromPulsarDoFn.class);
    private PulsarClient client;
    private PulsarAdmin admin;
    private String clientUrl;
    private String adminUrl;

    @VisibleForTesting Reader<byte[]> readerTst;

    private final SerializableFunction<Message<byte[]>, Instant> extractOutputTimestampFn;


    public ReadFromPulsarDoFn(PulsarIO.Read transform) {
        this.extractOutputTimestampFn = transform.getExtractOutputTimestampFn();
        this.clientUrl = transform.getClientUrl();
        this.adminUrl = transform.getAdminUrl();
    }

    // Open connection to Pulsar clients
    @Setup
    public void initPulsarClients() throws Exception {
        if(this.clientUrl == null) {
            this.clientUrl = PulsarIOUtils.SERVICE_URL;
        }
        if(this.adminUrl == null) {
            this.adminUrl = PulsarIOUtils.SERVICE_HTTP_URL;
        }
        if(this.client == null) {
            this.client = PulsarClient.builder()
                    .serviceUrl(clientUrl)
                    .build();
        }
        if(this.admin == null) {
            this.admin = PulsarAdmin.builder()
                    .serviceHttpUrl(adminUrl)
                    .tlsTrustCertsFilePath(null)
                    .allowTlsInsecureConnection(false)
                    .build();
        }
    }

    @VisibleForTesting
    public void setReader(Reader<byte[]> reader) throws Exception {
        this.readerTst = reader;
    }

    // Close connection to Pulsar clients
    @Teardown
    public void teardown() throws Exception {
        this.client.close();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element PulsarSourceDescriptor pulsarSource) {
        long startTimestamp = 0L;
        long endTimestamp = Long.MAX_VALUE;

        if(pulsarSource.getStartOffset() != null) {
            startTimestamp = pulsarSource.getStartOffset();
        }

        if(pulsarSource.getEndOffset() != null) {
            endTimestamp = pulsarSource.getEndOffset();
        }

        return new OffsetRange(startTimestamp, endTimestamp);
    }

    /*
    It may define a DoFn.GetSize method or ensure that the RestrictionTracker implements
    RestrictionTracker.HasProgress. Poor auto-scaling of workers and/or splitting may result
     if size or progress is an inaccurate representation of work.
     See DoFn.GetSize and RestrictionTracker.HasProgress for further details.
     */
    @GetSize
    public double getSize(@Element PulsarSourceDescriptor pulsarSource, @Restriction OffsetRange range) throws PulsarAdminException {
        //TODO improve getsize estiamate, check pulsar stats to improve get size estimate
        // https://pulsar.apache.org/docs/en/admin-api-topics/#get-stats
        double estimateRecords = restrictionTracker(pulsarSource, range).getProgress().getWorkRemaining();
        return estimateRecords;
    }

    private Reader<byte[]> newReader(PulsarClient client, String topicPartition) throws PulsarClientException {
        if(this.readerTst != null) {
            return this.readerTst;
        }
        ReaderBuilder<byte[]> builder = client.newReader().topic(topicPartition).startMessageId(MessageId.earliest);
        return builder.create();
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> getRestrictionCoder() {
        return new OffsetRange.Coder();
    }

    @ProcessElement
    public ProcessContinuation processElement(
            @Element PulsarSourceDescriptor pulsarSourceDescriptor,
            RestrictionTracker<OffsetRange, Long> tracker,
            WatermarkEstimator watermarkEstimator,
            OutputReceiver<PulsarMessage> output) throws IOException {
        long startTimestamp = tracker.currentRestriction().getFrom();
        String topicDescriptor = pulsarSourceDescriptor.getTopic();
        try (Reader<byte[]> reader = newReader(client, topicDescriptor)) {
            if (startTimestamp > 0 ) {
                reader.seek(startTimestamp);
            }
            while (true) {
                if (reader.hasReachedEndOfTopic()) {
                    reader.close();
                    return ProcessContinuation.stop();
                }
                Message<byte[]> message = reader.readNext();
                if (message == null) {
                    return ProcessContinuation.resume();
                }
                Long currentTimestamp = message.getPublishTime();
                // if tracker.tryclaim() return true, sdf must execute work otherwise
                // doFn must exit processElement() without doing any work associated
                // or claiming more work
                //System.out.println(new String(message.getValue(), StandardCharsets.UTF_8));
                if (!tracker.tryClaim(currentTimestamp)) {
                    reader.close();
                    return ProcessContinuation.stop();
                }
                if(pulsarSourceDescriptor.getEndMessageId() != null) {
                    MessageId currentMsgId = message.getMessageId();
                    boolean hasReachedEndMessageId = currentMsgId.compareTo(pulsarSourceDescriptor.getEndMessageId()) == 0;
                    if(hasReachedEndMessageId) return ProcessContinuation.stop();
                }
                PulsarMessage pulsarMessage = new PulsarMessage(message.getTopicName(), message.getPublishTime(), message);
                Instant outputTimestamp = extractOutputTimestampFn.apply(message);
                output.outputWithTimestamp(pulsarMessage, outputTimestamp);
            }
        }
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
        return currentElementTimestamp;
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
            @WatermarkEstimatorState Instant watermarkEstimatorState) {
        return new WatermarkEstimators.MonotonicallyIncreasing(ensureTimestampWithinBounds(watermarkEstimatorState));
    }

    @NewTracker
    public OffsetRangeTracker restrictionTracker(@Element PulsarSourceDescriptor pulsarSource,
                                                         @Restriction OffsetRange restriction) throws PulsarAdminException {
        if(restriction.getTo() < Long.MAX_VALUE) {
            return new OffsetRangeTracker(restriction);
        }

        PulsarLatestOffsetEstimator offsetEstimator =  new PulsarLatestOffsetEstimator(this.admin, pulsarSource.getTopic());
        return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetEstimator);

    }

    private static class PulsarLatestOffsetEstimator implements GrowableOffsetRangeTracker.RangeEndEstimator {

        private final Supplier<Message> memoizedBacklog;
        private PulsarLatestOffsetEstimator(PulsarAdmin admin, String topic) {
              this.memoizedBacklog = Suppliers.memoizeWithExpiration(
                    () -> {
                        Message<byte[]> lastMsg = null;
                        try {
                            lastMsg = admin.topics().examineMessage(topic, "latest", 1);
                        } catch (PulsarAdminException e) {
                            LOGGER.error(e.getMessage());
                        }
                        return lastMsg;
                    } , 1,
                    TimeUnit.SECONDS);
        }

        @Override
        public long estimate() {
            Message<byte[]> msg = memoizedBacklog.get();
            return msg.getPublishTime();
        }
    }


    private static Instant ensureTimestampWithinBounds(Instant timestamp) {
        if(timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
            timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
        } else if(timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
            timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
        }
        return timestamp;
    }

}
