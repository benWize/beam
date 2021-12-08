package org.apache.beam.sdk.io.pulsar;

import org.apache.pulsar.client.api.Message;

/**
 * Class representing a Pulsar Message record. Each PulsarMessage contains a single message basic message data
 * and Message record to access directly.
 */
public class PulsarMessage {
    private String topic;
    private Long publishTimestamp;
    private Object messageRecord;

    public PulsarMessage(String topic, Long publishTimestamp) {
        this.topic = topic;
        this.publishTimestamp = publishTimestamp;
    }
    public PulsarMessage(String topic, Long publishTimestamp, Object messageRecord) {
        this.topic = topic;
        this.publishTimestamp = publishTimestamp;
        this.messageRecord = messageRecord;
    }

    public String getTopic() {
        return topic;
    }

    public Long getPublishTimestamp() {
        return publishTimestamp;
    }

    public Object getMessageRecord() {
        return messageRecord;
    }
}
