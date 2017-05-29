package hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class GlobalHBaseMinicluster {

    private static final Log LOG = LogFactory.getLog(GlobalHBaseMinicluster.class);
    private static HBaseTestingUtility hbaseTestUtl = null;
    private static MiniClusterShutdownThread shutdownThread = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            hbaseTestUtl = new HBaseTestingUtility(conf);
            LOG.info("starting minicluster");
            hbaseTestUtl.startMiniCluster();
            LOG.info("set shutdown thread");
            shutdownThread = new MiniClusterShutdownThread();
            Runtime.getRuntime().addShutdownHook(shutdownThread);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private GlobalHBaseMinicluster() {}

    public static HBaseTestingUtility getHbaseTestUtl() {
        if (hbaseTestUtl == null || hbaseTestUtl.getDFSCluster() == null) {
            LOG.error("global minicluter not already!");
            throw new RuntimeException("global minicluster not running!");
        }
        return hbaseTestUtl;
    }

    private static class MiniClusterShutdownThread extends Thread {

        @Override
        public synchronized void run() {
            if (hbaseTestUtl != null) {
                try {
                    LOG.info("stoping minicluster");
                    hbaseTestUtl.shutdownMiniCluster();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
