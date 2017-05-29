package client;

import coprocessor.HbaseCoprocessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class HQueueAdmin implements Abortable, Closeable {
    private final static int TTL = 3600 * 24;// one day
    private final static String COPROCESSOR_JAR_PATH = "/userCopro-1.0-SNAPSHOT.jar";
    private Admin admin;
    public HQueueAdmin() throws IOException{
        Configuration conf = new Configuration();
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(conf));
        this.admin = connection.getAdmin();
    }

    public void createHQueue(String name, int partitionCount) throws IOException {
        createHQueue(name, partitionCount, TTL);
    }

    public void createHQueue(String name, int partitionCount, int ttl) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(name));
        hTableDescriptor.setDurability(Durability.ASYNC_WAL);
        hTableDescriptor.setMaxFileSize(Long.MAX_VALUE);
        hTableDescriptor.setMemStoreFlushSize(256 * 1024 * 1024);

        Path path = new Path(COPROCESSOR_JAR_PATH);
        hTableDescriptor.addCoprocessor(HbaseCoprocessor.class.getName(),path,
                100, null);

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(HQueueConstants.COLUMN_FAMILY);
        hColumnDescriptor.setBlockCacheEnabled(false);
        hColumnDescriptor.setBlocksize(1024 * 1024);
        hColumnDescriptor.setCompactionCompressionType(Compression.Algorithm.NONE);
        hColumnDescriptor.setCompressionType(Compression.Algorithm.NONE);
        hColumnDescriptor.setDataBlockEncoding(DataBlockEncoding.DIFF);
        hColumnDescriptor.setMaxVersions(1);
        hColumnDescriptor.setTimeToLive(ttl);
        hColumnDescriptor.setValue(HStore.BLOCKING_STOREFILES_KEY, String.valueOf(Integer.MAX_VALUE));

        hTableDescriptor.addFamily(hColumnDescriptor);


        admin.createTable(hTableDescriptor, generateSplitKeys(partitionCount));
    }

    private byte[][] generateSplitKeys(int partitionCount){
        byte[][] keys = new byte[partitionCount - 1][];
        for(int i = 0; i < partitionCount - 1; ++i){
            keys[i] = Bytes.toBytes(i + 1);
        }
        return keys;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void abort(String s, Throwable throwable) {

    }

    @Override
    public boolean isAborted() {
        return false;
    }
}
