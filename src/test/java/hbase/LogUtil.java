package hbase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LogUtil {

    public static void addjustLogLevel() throws Exception {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
        Logger.getLogger("BlockStateChange").setLevel(Level.ERROR);
        Logger.getLogger("org.mortbay").setLevel(Level.ERROR);
    }

}
