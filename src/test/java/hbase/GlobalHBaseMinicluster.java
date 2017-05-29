package hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class GlobalHBaseMinicluster {

    private static final Log LOG = LogFactory.getLog(GlobalHBaseMinicluster.class);
    private static HBaseTestingUtility miniCluster = null;
    private static MiniClusterShutdownThread shutdownThread = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
//            conf.set("hbase.master.info.port", Integer.valueOf(PortHelper.getPort()).toString());
//            conf.set("hbase.regionserver.info.port", Integer.valueOf(PortHelper.getPort())
//                    .toString());
            miniCluster = new HBaseTestingUtility(conf);
            LOG.info("starting minicluster");
            miniCluster.startMiniCluster();
            LOG.info("set shut down thread");
            shutdownThread = new MiniClusterShutdownThread();
            Runtime.getRuntime().addShutdownHook(shutdownThread);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private GlobalHBaseMinicluster() {}

    public static HBaseTestingUtility getMiniCluster() {
        if (miniCluster == null || miniCluster.getDFSCluster() == null) {
            LOG.error("global minicluter shoud be start already!");
            throw new RuntimeException("global minicluster is not running!");
        }
        return miniCluster;
    }

    private static class MiniClusterShutdownThread extends Thread {

        @Override
        public synchronized void run() {
            if (miniCluster != null) {
                try {
                    LOG.info("stoping minicluster");
                    miniCluster.shutdownMiniCluster();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

}
