package coprclient;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * Created by zhuifeng on 2017/5/25.
 */
public class PartitionScanner implements Cloneable{
    private ResultScanner scanner;
    private Message next;
    public PartitionScanner(ResultScanner scanner){
        this.scanner = scanner;
    }

    public boolean hasNext() throws IOException {
        Result result = scanner.next();
        if(null == result)
            return false;
        next = MessageProxy.result2Message(result);
        return next != null;
    }

    public Message next() {
        return next;
    }

    public void close(){
        scanner.close();
    }
}
