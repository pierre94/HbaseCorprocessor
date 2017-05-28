package client;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.NavigableMap;

import static client.HQueueConstants.*;

/**
 * Created by zhuifeng on 2017/5/25.
 */
public class MessageProxy {
    public static byte[] makeRowkey(Message message){
        byte[] rowkey = new byte[ROW_KEY_LENGTH];
        System.arraycopy(Bytes.toBytes(message.getPartitionId()), 0 ,rowkey, 0, PARTITION_ID_LENGTH);
        System.arraycopy(Bytes.toBytes(message.getMessageId().getTimestamp()), 0,
                rowkey, PARTITION_ID_LENGTH, TIMESTAMP_LENGTH);
        System.arraycopy(Bytes.toBytes(message.getMessageId().getSequenceID()), 0,
                rowkey, PARTITION_ID_LENGTH + TIMESTAMP_LENGTH, SEQUENCE_ID_LENGTH);
        return rowkey;
    }

    public static Message result2Message(Result result){
        Cell cell = result.listCells().get(0);
        byte[] rowkey = cell.getRowArray();
        short partitionId = Bytes.toShort(rowkey, 0 , PARTITION_ID_LENGTH);
        long timestamp = Bytes.toLong(rowkey, PARTITION_ID_LENGTH , TIMESTAMP_LENGTH);
        short sequenceId = Bytes.toShort(rowkey, PARTITION_ID_LENGTH + TIMESTAMP_LENGTH , SEQUENCE_ID_LENGTH);
        MessageId messageId = new MessageId(timestamp, partitionId);
        byte[] topic = cell.getQualifierArray();
        byte[] value = cell.getValueArray();
        return new Message(partitionId, messageId, topic, value);
    }
}
