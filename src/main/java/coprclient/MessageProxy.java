package coprclient;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import static coprclient.HQueueConstants.*;

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
        short partitionId = Bytes.toShort(cell.getRowArray(), cell.getRowOffset() , PARTITION_ID_LENGTH);
        long timestamp = Bytes.toLong(cell.getRowArray(),
                cell.getRowOffset() + PARTITION_ID_LENGTH , TIMESTAMP_LENGTH);
        short sequenceId = Bytes.toShort(cell.getRowArray(),
                cell.getRowOffset() +  PARTITION_ID_LENGTH + TIMESTAMP_LENGTH , SEQUENCE_ID_LENGTH);
        MessageId messageId = new MessageId(timestamp, sequenceId);
        byte[] topic = CellUtil.cloneQualifier(cell);
        byte[] value = CellUtil.cloneValue(cell);
        return new Message(partitionId, messageId, topic, value);
    }
}
