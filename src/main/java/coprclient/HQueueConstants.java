package coprclient;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zhuifeng on 2017/5/25.
 */
public class HQueueConstants {
    public final static String COLUMN_FAMILY = "q";
    public final static byte[] DEFAULT_TOPIC = Bytes.toBytes("d");
    public final static short PARTITION_ID_LENGTH = Bytes.SIZEOF_SHORT;
    public final static short TIMESTAMP_LENGTH = Bytes.SIZEOF_LONG;
    public final static short SEQUENCE_ID_LENGTH = Bytes.SIZEOF_SHORT;
    public final static short ROW_KEY_LENGTH = PARTITION_ID_LENGTH + TIMESTAMP_LENGTH + SEQUENCE_ID_LENGTH;
}
