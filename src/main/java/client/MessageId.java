package client;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.util.Random;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class MessageId {
    private long timestamp;
    private short sequenceID;
    private final static Random RAND = new Random();

    public MessageId() {
        this.timestamp = EnvironmentEdgeManager.currentTime();
        this.sequenceID = (short)RAND.nextInt(Short.MAX_VALUE);
    }

    public MessageId(long timestamp) {
        this.timestamp = timestamp;
        this.sequenceID = (short)RAND.nextInt(Short.MAX_VALUE);
    }

    public MessageId(short sequenceID) {
        this.timestamp = EnvironmentEdgeManager.currentTime();
        this.sequenceID = sequenceID;
    }

    public MessageId(long timestamp, short sequenceID) {
        this.timestamp = timestamp;
        this.sequenceID = sequenceID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public short getSequenceID() {
        return sequenceID;
    }

    public void setSequenceID(short sequenceID) {
        this.sequenceID = sequenceID;
    }

    @Override
    public String toString() {
        return "{timestamp=" + timestamp + ", sequenceID=" + sequenceID + "}";
    }
}
