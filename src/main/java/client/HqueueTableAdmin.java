package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by zhuifeng on 2017/5/23.
 */
public class HqueueTableAdmin implements Abortable, Closeable {
    private Admin admin;
    public HqueueTableAdmin() throws IOException{
        Configuration conf = new Configuration();
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(conf));
        this.admin = connection.getAdmin();
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
