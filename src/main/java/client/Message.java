package client;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class Message {
    private short partitionId;
    private MessageId messageId;
    private byte[] topic;
    private byte[] value;

    private static byte[] DEFAULT_TOPIC = Bytes.toBytes("d");

    public Message(short partitionId, byte[] value) {
        this(partitionId, new MessageId(), DEFAULT_TOPIC, value);
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
}
