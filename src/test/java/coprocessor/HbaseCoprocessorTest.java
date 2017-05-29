package coprocessor;

import client.HQueue;
import client.HQueueAdmin;
import client.Message;
import client.PartitionScanner;
import hbase.GlobalHBaseMinicluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by zhuifeng on 2017/5/29.
 */
public class HbaseCoprocessorTest {
    public static HBaseTestingUtility miniCluster = GlobalHBaseMinicluster.getMiniCluster();
    public static HBaseAdmin hBaseAdmin = null;
    public final static String HQUEUE_NAME = "first_queue";
    public final static short PARTTION_COUNT = 5;
    public final static short PARTTION_Id = 2;
    @Before
    public void setUp() throws Exception {
        hBaseAdmin = miniCluster.getHBaseAdmin();
    }

    @After
    public void tearDown(){
        try {
            hBaseAdmin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void preBatchMutate() throws Exception {
        HQueueAdmin admin = new HQueueAdmin(hBaseAdmin);
        admin.createHQueue(HQUEUE_NAME, PARTTION_COUNT, 3600 * 24, true);
        HQueue hqueue = new HQueue(HQUEUE_NAME, hBaseAdmin);
        Message message = new Message(PARTTION_Id, Bytes.toBytes("testvalue"));
        hqueue.put(message);

        PartitionScanner scanner = hqueue.getScanner(PARTTION_Id);
        while (scanner.hasNext()){
            Message scanMessage = scanner.next();
            System.out.println(scanMessage);
        }
        System.out.println("+++++++++++++++++");

    }

}