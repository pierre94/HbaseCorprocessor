package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class HQueue {
    private Table table;

    public HQueue(String name) throws IOException {
        Configuration conf = new Configuration();
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(conf));
        table = connection.getTable(TableName.valueOf(name));
    }

    public PartitionScanner getScanner(byte [] topic)
            throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(HQueueConstants.COLUMN_FAMILY), topic);
        return new PartitionScanner(table.getScanner(scan));
    }

    public void put(Message message){}
    public void put(List<Message> messages){}
}
