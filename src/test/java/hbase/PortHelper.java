package hbase;

import java.io.IOException;
import java.net.ServerSocket;

public class PortHelper {
    public synchronized static int getPort()  throws IOException{
        int port;
        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();
        return port;
    }
}
