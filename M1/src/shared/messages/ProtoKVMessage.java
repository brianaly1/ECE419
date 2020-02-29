package shared.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.protobuf.ExtensionRegistryLite;
import shared.messages.ProtobufMessage.ProtobufKVMessage;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.CodedInputStream;

// This is the protobuf-KVMessage implementation which relies on protocol buffer.
// See https://developers.google.com/protocol-buffers/.
public class ProtoKVMessage implements KVMessage {

    ProtobufKVMessage message = null;

    public ProtoKVMessage() {}

    public ProtoKVMessage(String key, String value, StatusType status) {
        message = ProtobufKVMessage.newBuilder()
                .setKey(key)
                .setValue(value)
                .setStatusType(status.ordinal())
                .build();
    }

    @Override
    public String getKey() {
        return message.getKey();
    }

    @Override
    public String getValue() {
        return message.getValue();
    }

    @Override
    public StatusType getStatus() {

        return StatusType.values()[message.getStatusType()];
    }

    public void writeMessage(OutputStream out) throws IOException {
        try {
            message.writeDelimitedTo(out);
        } catch (Exception e) {
            throw e;
        }
    }

    public void parseMessage(InputStream in) throws Exception{
        try {
            message = ProtobufKVMessage.parseDelimitedFrom(in);
        } catch (Exception e) {
            throw e;
        }
    }

    public String toString() {
        return message.toString();
    }

    public ProtobufKVMessage getProtobuf() {
        return message;
    }

}
