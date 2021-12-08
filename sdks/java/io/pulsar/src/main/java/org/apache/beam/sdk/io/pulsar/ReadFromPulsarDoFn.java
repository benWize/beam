package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.pulsar.client.api.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

@DoFn.UnboundedPerElement
public class ReadFromPulsarDoFn extends DoFn<PulsarSourceDescriptor, PulsarMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFromPulsarDoFn.class);
    private PulsarClient client;
    private String topic;
    private String clientUrl;

    private final SerializableFunction<Message<byte[]>, Instant> extractOutputTimestampFn;

    ReadFromPulsarDoFn(PulsarIO.Read transform) {
        this.extractOutputTimestampFn = transform.getExtractOutputTimestampFn();
        this.clientUrl = transform.getClientUrl();
        this.topic = transform.getTopic();

    }

    // Open connection to Pulsar clients
    @Setup
    public void initPulsarClients() throws Exception {
        if(this.clientUrl == null) {
            this.clientUrl = PulsarIOUtils.SERVICE_URL;
        }
        if(this.client == null) {
            this.client = PulsarClient.builder()
                    .serviceUrl(clientUrl)
                    .build();
        }
    }

    // Close connection to Pulsar clients
    @Teardown
    public void teardown() throws Exception {
        this.client.close();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element PulsarSourceDescriptor pulsarSource) {
        // Reading a topic from starting point with offset 0
        long startOffset = 0;
        if(pulsarSource.getStartOffset() != null) {
            startOffset = pulsarSource.getStartOffset();
        }

        return new OffsetRange(startOffset, Long.MAX_VALUE);
    }

    /*
    It may define a DoFn.GetSize method or ensure that the RestrictionTracker implements
    RestrictionTracker.HasProgress. Poor auto-scaling of workers and/or splitting may result
     if size or progress is an inaccurate representation of work.
     See DoFn.GetSize and RestrictionTracker.HasProgress for further details.
     */
    @GetSize
    public double getSize(@Element PulsarSourceDescriptor pulsarSource, @Restriction OffsetRange offsetRange) throws PulsarClientException {
        //TODO improve getsize estiamate, check pulsar stats to improve get size estimate
        // https://pulsar.apache.org/docs/en/admin-api-topics/#get-stats
        double estimateRecords = restrictionTracker(pulsarSource, offsetRange).getProgress().getWorkRemaining();
        return estimateRecords;
    }

    private Reader<byte[]> newReader(PulsarClient client, String topicPartition) throws PulsarClientException {
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
            OutputReceiver<PulsarMessage> output) throws IOException {
        //initPulsarClients();
        long startTimestamp = tracker.currentRestriction().getFrom();
        String topicPartition = pulsarSourceDescriptor.getTopic();
        //List<String> topicPartitions = client.getPartitionsForTopic(pulsarRecord.getTopic()).join();
        //for(String topicPartition:topicPartitions) {
        try (Reader<byte[]> reader = newReader(client, topicPartition)) {
            if (startTimestamp != 0) {
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
                long currentTimestampOffset = message.getPublishTime();
                // if tracker.tryclaim() return true, sdf must execute work otherwise
                // doFn must exit processElement() without doing any work associated
                // or claiming more work
                System.out.println(new String(message.getValue(), StandardCharsets.UTF_8));
                System.out.println(currentTimestampOffset);
                if (!tracker.tryClaim(currentTimestampOffset)) {
                    reader.close();
                    return ProcessContinuation.stop();
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
    public GrowableOffsetRangeTracker restrictionTracker(@Element PulsarSourceDescriptor pulsarSource,
                                                         @Restriction OffsetRange restriction) throws PulsarClientException {

        PulsarLatestOffsetEstimator offsetEstimator =  new PulsarLatestOffsetEstimator(client, pulsarSource.getTopic());
        return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetEstimator);

    }

    private static class PulsarLatestOffsetEstimator implements GrowableOffsetRangeTracker.RangeEndEstimator {

        private final Supplier<Message<byte[]>> memoizedBacklog;
        private final Consumer<byte[]> readerLatestMsg;

        private PulsarLatestOffsetEstimator(PulsarClient client, String topic, int consumerNo) throws PulsarClientException {
            this.readerLatestMsg = client.newConsumer()
                                        .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                                        .subscriptionName("PulsarLatestOffsetEstimator")
                                        .topic(topic)
                                        .subscribe();
            this.memoizedBacklog = Suppliers.memoizeWithExpiration(
                    () -> {
                        Message<byte[]> latestMessage = null;
                        try {
                            latestMessage = readerLatestMsg.receive();
                        } catch (Exception e) {
                            //TODO change error log
                            //e.printStackTrace();
                        }
                        return latestMessage;
                    }, 1,
                    TimeUnit.SECONDS);
        }

        @Override
        protected void finalize() {
            try {
                this.readerLatestMsg.close();
            } catch (IOException e) {
                //TODO add error log
                //e.printStackTrace();
            }
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
