package coprclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class PutCoprocessorTests extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(PutCoprocessorTests.class);
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(PutCoprocessorTests.class);
    private long timestamp = 0;
    private short sequenceId = 0;

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        LOG.info("open test coprocessor");
        log.info("open test coprocessor");
        System.out.println("open test coprocessor");
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        KeyValue kv = new KeyValue(get.getRow(),Bytes.toBytes(HQueueConstants.COLUMN_FAMILY),
                HQueueConstants.DEFAULT_TOPIC, Bytes.toBytes("abcdefg"));
        results.add(kv);
        e.bypass();
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        //use preBatchMutate instead of prePut for this lock
        synchronized(this){
            LOG.info("come into preBatchMutate");
            log.info("come into preBatchMutate");
            System.out.println("come into preBatchMutate");
            long currentTime = EnvironmentEdgeManager.currentTime();
            if(timestamp != currentTime){
                timestamp = currentTime;
                sequenceId = 0;
            }
            for(int i = 0; i < miniBatchOp.size(); ++i){
                if(miniBatchOp.getOperationStatus(i).getOperationStatusCode() != HConstants.OperationStatusCode.NOT_RUN){
                    LOG.info("preBatchMutate is be continued");
                    log.info("preBatchMutate is be continued");
                    System.out.println("preBatchMutate is be continued");
                    continue;
                }
                Mutation mutation = miniBatchOp.getOperation(i);
                if(mutation instanceof Put){
                    if (sequenceId == Short.MAX_VALUE) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        timestamp = EnvironmentEdgeManager.currentTime();
                        sequenceId = 0;
                    }
                    tranformPutMessageId((Put)mutation, timestamp, sequenceId++);
                }
            }
        }
    }

    private void tranformPutMessageId(Put put, long timestamp, short sequenceId){
        LOG.info("timestamp is:"+timestamp);
        log.info("timestamp is:"+timestamp);
        System.out.println("timestamp is:"+timestamp);
        LOG.info("sequenceId is:"+sequenceId);
        log.info("sequenceId is:"+sequenceId);
        System.out.println("sequenceId is:"+sequenceId);
        System.arraycopy(Bytes.toBytes(timestamp), 0, put.getRow(),
                HQueueConstants.PARTITION_ID_LENGTH, HQueueConstants.TIMESTAMP_LENGTH);
        System.arraycopy(Bytes.toBytes(sequenceId), 0, put.getRow(),
                HQueueConstants.PARTITION_ID_LENGTH + HQueueConstants.TIMESTAMP_LENGTH,
                HQueueConstants.SEQUENCE_ID_LENGTH);
        for(List<Cell> cells : put.getFamilyCellMap().values()){
            for(Cell cell : cells){
                System.arraycopy(Bytes.toBytes(timestamp), 0, cell.getRowArray(),
                        cell.getRowOffset() + HQueueConstants.PARTITION_ID_LENGTH, HQueueConstants.TIMESTAMP_LENGTH);
                System.arraycopy(Bytes.toBytes(sequenceId), 0, cell.getRowArray(),
                        cell.getRowOffset() + HQueueConstants.PARTITION_ID_LENGTH + HQueueConstants.TIMESTAMP_LENGTH,
                        HQueueConstants.SEQUENCE_ID_LENGTH);
                System.arraycopy(Bytes.toBytes("11111"), 0, cell.getValueArray(), cell.getValueOffset(),
                        Bytes.toBytes("11111").length);
            }
        }
    }
}
