import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class MockPulsarReader implements Reader<byte[]> {

    private String topic;
    private List<MockMessage> mockMessages = new ArrayList<>();
    private int currentMsg;
    private long startTimestamp;
    private long endTimestamp;
    private boolean reachedEndOfTopic;
    private int numberOfMessages;

    public MockPulsarReader() {}

    public MockPulsarReader(String topic, int numberOfMessages) {
        this.numberOfMessages = numberOfMessages;
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
        setMock(topic, numberOfMessages);
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
