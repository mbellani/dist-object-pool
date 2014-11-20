package org.mbellani;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mbellani.zk.ZKClient;
import org.mbellani.zk.ZKClient.SynchronizedOperationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoSynchronizeTest {

    private Thread t1;
    private Thread t2;
    private String lockPath = "/test-lock-path";
    private ZKClient client;

    @Before
    public void setUp() throws Exception {
        ZkServer.start();
        initializeZkClient();
        if (client.exists(lockPath)) {
            client.delete(lockPath);
        }
        client.create(lockPath);
        createThreads();
        waitForTasksToFinish();
    }

    private void waitForTasksToFinish() throws InterruptedException {
        t1.join();
        t2.join();
    }

    private void createThreads() throws Exception {
        t1 = new Thread(new SynchronousTask(lockPath));
        t2 = new Thread(new SynchronousTask(lockPath));
        t1.start();
        t2.start();
    }

    private void initializeZkClient() throws IOException, InterruptedException {
        client = new ZKClient(ZkServer.connectString());
    }

    @After
    public void tearDown() throws Exception {
        client.delete(lockPath);
    }

    @Test
    public void test() {
    }

    public class SynchronousTask implements Runnable {
        private final Logger LOGGER = LoggerFactory.getLogger(SynchronousTask.class);
        private ZKClient client;
        private String path;

        public SynchronousTask(String path) throws Exception {
            this.client = new ZKClient(ZkServer.connectString());
            this.path = path;
        }

        @Override
        public void run() {
            client.doSynchronized(path, new SynchronizedOperationCallback<Object>() {
                @Override
                public Object perform() throws InterruptedException, KeeperException {
                    LOGGER.info("Performing Synchronous Operation.");
                    Thread.sleep(5000);
                    LOGGER.info("Synchronous operation finished.");
                    return new Object();
                }
            });
        }
    }

}
