package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.pulsar.client.api.*;

public class PulsarWriter extends DoFn<byte[], Void> {

    private Producer<byte[]> producer;
    private PulsarClient client;
    private String clientUrl;
    private String topic;

    PulsarWriter(PulsarIO.Write transform) {
        this.clientUrl = transform.getClientUrl();
        this.topic = transform.getTopic();
    }

    @Setup
    public void setup() throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl(clientUrl)
                .build();

        producer = client.newProducer()
                .topic(topic)
                .compressionType(CompressionType.LZ4)
                .create();

    }

    @ProcessElement
    public void processElement(ProcessContext ctx) throws Exception {
        byte[] message = ctx.element();
        producer.send(message);
    }

    @Teardown
    public void teardown() throws PulsarClientException {
        producer.close();
    }
}
