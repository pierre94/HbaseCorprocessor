package coprclient;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class Message {
    private short partitionId;
    private MessageId messageId;
    private byte[] topic;
    private byte[] value;

    public Message(short partitionId, byte[] value) {
        this(partitionId, new MessageId(), HQueueConstants.DEFAULT_TOPIC, value);
    }

    public Message(short partitionId, MessageId messageId) {
        this.partitionId = partitionId;
        this.messageId = messageId;
    }


    public Message(short partitionId, byte[] topic, byte[] value) {
        this(partitionId, new MessageId(), topic, value);
    }

    public Message(short partitionId, MessageId messageId, byte[] topic, byte[] value) {
        this.partitionId = partitionId;
        this.messageId = messageId;
        this.topic = topic;
        this.value = value;
    }

    public short getPartitionId() {
        return partitionId;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public byte[] getTopic() {
        return topic;
    }

    public byte[] getValue() {
        return value;
    }

    public String toString() {
        return String.format(
                "partitionID(%d), timestamp(%d) sequence(%d) topic(%s) value(%s)", partitionId,
                messageId.getTimestamp(), messageId.getSequenceID(), Bytes.toString(topic), Bytes.toString(value));
    }
}
