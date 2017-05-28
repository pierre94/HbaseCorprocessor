package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class HQueue implements Closeable {
    private Table table;

    public HQueue(String name) throws IOException {
        Configuration conf = new Configuration();
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(conf));
        table = connection.getTable(TableName.valueOf(name));
    }

    public PartitionScanner getScanner(short partitionId)
            throws IOException {
        return getScanner(partitionId, 0L, HQueueConstants.DEFAULT_TOPIC);
    }

    public PartitionScanner getScanner(short partitionId, long timestamp)
            throws IOException {
        return getScanner(partitionId, timestamp, HQueueConstants.DEFAULT_TOPIC);
    }

    public PartitionScanner getScanner(short partitionId, byte [] topic)
            throws IOException {
        return getScanner(partitionId, 0L, topic);
    }

    public PartitionScanner getScanner(short partitionId, long timestamp, byte [] topic)
            throws IOException {
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(1024);
        byte[] startRow = new byte[HQueueConstants.ROW_KEY_LENGTH];
        System.arraycopy(Bytes.toBytes(partitionId), 0 ,  startRow,
                0, HQueueConstants.PARTITION_ID_LENGTH);
        System.arraycopy(Bytes.toBytes(timestamp), 0 ,  startRow,
                HQueueConstants.PARTITION_ID_LENGTH, HQueueConstants.TIMESTAMP_LENGTH);
        System.arraycopy(Bytes.toBytes(0), 0 ,  startRow,
                HQueueConstants.PARTITION_ID_LENGTH + HQueueConstants.TIMESTAMP_LENGTH, HQueueConstants.SEQUENCE_ID_LENGTH);
        byte[] stopRow = new byte[HQueueConstants.ROW_KEY_LENGTH];
        System.arraycopy(Bytes.toBytes(partitionId), 0 ,  stopRow,
                0, HQueueConstants.PARTITION_ID_LENGTH);
        System.arraycopy(Bytes.toBytes(Long.MAX_VALUE), 0 ,  stopRow,
                HQueueConstants.PARTITION_ID_LENGTH, HQueueConstants.TIMESTAMP_LENGTH);
        System.arraycopy(Bytes.toBytes(Short.MAX_VALUE), 0 ,  stopRow,
                HQueueConstants.PARTITION_ID_LENGTH + HQueueConstants.TIMESTAMP_LENGTH, HQueueConstants.SEQUENCE_ID_LENGTH);

        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
        scan.addColumn(Bytes.toBytes(HQueueConstants.COLUMN_FAMILY), topic);
        return new PartitionScanner(table.getScanner(scan));
    }

    public void put(Message message) throws IOException {
        Put put = new Put(MessageProxy.makeRowkey(message));
        put.addColumn(Bytes.toBytes(HQueueConstants.COLUMN_FAMILY), message.getTopic(), message.getValue());
        table.put(put);
    }
    public void put(List<Message> messages) throws IOException {
        List<Put> list = new ArrayList<>();
        for(Message message : messages){
            Put put = new Put(MessageProxy.makeRowkey(message));
            put.addColumn(Bytes.toBytes(HQueueConstants.COLUMN_FAMILY), message.getTopic(), message.getValue());
            list.add(put);
        }
        table.put(list);
    }

    @Override
    public void close() throws IOException {
        if(null != table){
            table.close();
            table = null;
        }
    }
}
