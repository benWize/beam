package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.coders.*;
import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PulsarMessageCoder extends CustomCoder<PulsarMessage> {

    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private static final VarLongCoder longCoder = VarLongCoder.of();
    //private final Coder<Message<byte[]>> messageCoder;

    public static PulsarMessageCoder of() {
        return new PulsarMessageCoder();
    }

    public PulsarMessageCoder() {}

    @Override
    public void encode(PulsarMessage value, OutputStream outStream) throws CoderException, IOException {
        stringCoder.encode(value.getTopic(), outStream);
        longCoder.encode(value.getPublishTimestamp(), outStream);
    }


    @Override
    public PulsarMessage decode(InputStream inStream) throws CoderException, IOException {
        return new PulsarMessage(stringCoder.decode(inStream), longCoder.decode(inStream));
    }

}
