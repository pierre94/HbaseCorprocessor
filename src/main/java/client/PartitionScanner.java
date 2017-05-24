package client;

import org.apache.hadoop.hbase.client.ResultScanner;
/**
 * Created by zhuifeng on 2017/5/25.
 */
public class PartitionScanner {
    private ResultScanner scanner;
    public PartitionScanner(ResultScanner scanner){
        this.scanner = scanner;
    }
}
