package org.mbellani;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkServer {
    private static NIOServerCnxnFactory standaloneServerFactory;
    private static ZooKeeperServer server;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkServer.class);

    public static void start() throws Exception {
        if (standaloneServerFactory == null) {
            String dataDirectory = System.getProperty("java.io.tmpdir");
            File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
            server = new ZooKeeperServer(dir, dir, 2000);
            standaloneServerFactory = new NIOServerCnxnFactory();
            int port = freePort();
            LOGGER.info("Starting on port {}", port);
            standaloneServerFactory.configure((new InetSocketAddress(port)), 5000);
            standaloneServerFactory.startup(server); // start the server.
            LOGGER.info("Started server on " + connectString());
        }
    }

    public static int freePort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        int port = s.getLocalPort();
        s.close();
        return port;
    }

    public static String connectString() {
        return String.format("127.0.0.1:%s", standaloneServerFactory.getLocalPort());
    }

    public static void shutdown() {
        if (standaloneServerFactory != null) {
            standaloneServerFactory.shutdown();
            server.shutdown();
            standaloneServerFactory = null;
            server = null;
        }
    }

}
