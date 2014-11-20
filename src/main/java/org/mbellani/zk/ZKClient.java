package org.mbellani.zk;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.recipes.lock.WriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class ZKClient {
    private static final String DEFAULT_CONNECT_STRING = "127.0.0.1:2181";
    private static final int SESSION_TIMEOUT = 1000 * 10;
    private String connectString = DEFAULT_CONNECT_STRING;
    private ZooKeeper zookeeper;
    private CountDownLatch connLatch = null;
    private List<WriteLock> locks = Lists.newArrayList();
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKClient.class);

    public ZKClient(String connectString) throws IOException, InterruptedException {
        this.connectString = connectString;
    }

    public void sync(final String path, final Object context) throws InterruptedException {
        final CountDownLatch syncPending = new CountDownLatch(1);
        getZk().sync(path, new VoidCallback() {
            public void processResult(int rc, String path, Object ctx) {
                syncPending.countDown();
            }

        }, context);
        syncPending.await();
    }

    protected synchronized ZooKeeper getZk() {
        if (zookeeper == null || zookeeper.getState() == States.CLOSED) {
            checkState(!Strings.isNullOrEmpty(connectString), "Please specify a valid connect String.");
            zookeeper = null;
            connLatch = new CountDownLatch(1);
            try {
                zookeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, watch());
                waitToConnect();
            }
            catch (UnknownHostException ex) {
                LOGGER.error("Error resolving host, however the address worked earlier", ex);
                LOGGER.warn("attempting reconnect in 5 seconds");
                sleep(5000);
                getZk();
            }
            catch (Exception e) {
                Throwables.propagate(e);
            }
        }
        return zookeeper;

    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    private void waitToConnect() {
        if (connLatch.getCount() == 0) {
            return;
        }

        LOGGER.info("waiting for ZK connection...");
        try {
            final int secondsToWait = 30;
            if (!connLatch.await(secondsToWait, TimeUnit.SECONDS)) {
                LOGGER.info("waited {} seconds but we never finished connecting to ZK...", secondsToWait);
            }
        }
        catch (InterruptedException ex) {
            // this is fine
        }
    }

    private Watcher watch() {
        return new Watcher() {
            public void process(WatchedEvent e) {
                final KeeperState state = e.getState();
                if (e.getType() == Event.EventType.None && e.getState() == KeeperState.SyncConnected) {
                    connLatch.countDown();
                }
                else if (state == KeeperState.Expired || state == KeeperState.Disconnected) {
                    LOGGER.warn("session expired, forcing zk reconnect...");
                    forceReconnect();
                }
            }
        };
    }

    private synchronized void forceReconnect() {
        try {
            close();
        }
        catch (InterruptedException ex) {
            // this is fine
        }
    }

    public synchronized void close() throws InterruptedException {
        if (zookeeper != null) {
            zookeeper.close();
            zookeeper = null;
        }
    }

    public String createEphemeral(String path) throws KeeperException, InterruptedException {
        return getZk().create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public String createEphemeral(String path, byte[] data) throws KeeperException, InterruptedException {
        return getZk().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public String createEphemeralSeq(String path, byte[] data) throws KeeperException, InterruptedException {
        return getZk().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public String create(String path) throws KeeperException, InterruptedException {
        return getZk().create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String createSeq(String path, byte[] data) throws KeeperException, InterruptedException {
        return getZk().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public boolean exists(String path) {
        try {
            return getZk().exists(path, false) != null;
        }
        catch (ConnectionLossException e) {
            forceReconnect();
        }
        catch (KeeperException.NoNodeException e) {
            // path does not exist;
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        return false;
    }

    public Stat getStat(String path) {
        try {
            return getZk().exists(path, false);
        }
        catch (ConnectionLossException e) {
            forceReconnect();
        }
        catch (NoNodeException e) {
            // Do nothing the node may not be there.
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        return null;

    }

    public void delete(String path) throws NoNodeException {
        try {
            getZk().delete(path, -1);
        }
        catch (ConnectionLossException e) {
            forceReconnect();
        }
        catch (KeeperException.NoNodeException e) {
            throw e;
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    public List<String> getChildren(String path) {
        List<String> children = null;
        try {
            children = getZk().getChildren(path, false);
        }
        catch (ConnectionLossException e) {
            forceReconnect();
        }
        catch (Exception e) {
            Throwables.propagate(e);
            forceReconnect();
        }
        return children;
    }

    public Map<Stat, String> getChildrenStats(String parent) {
        Map<Stat, String> stats = new HashMap<Stat, String>();
        for (String child : getChildren(parent)) {
            Stat st = getStat(parent.concat("/").concat(child));
            if (st != null) {
                stats.put(st, child);
            }
        }
        return stats;
    }

    public List<String> getChildren(String path, Watcher watcher) {
        List<String> children = null;
        try {
            children = getZk().getChildren(path, watcher);
        }
        catch (ConnectionLossException e) {
            forceReconnect();
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        return children;
    }

    public byte[] getData(String path) {
        byte[] data = null;
        try {
            data = getZk().getData(path, null, null);
        }
        catch (ConnectionLossException e) {
            forceReconnect();
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        return data;
    }

    public ZKTransWrapper inTransaction() {
        return new ZKTransWrapper(this);
    }

    public <R> R doSynchronized(String lockPath, SynchronizedOperationCallback<R> callback) {
        R result = null;
        WriteLock lock = null;
        try {
            lock = aquireLock(lockPath);
            result = callback.perform();
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        finally {
            if (lock != null) {
                lock.unlock();
            }
        }
        return result;
    }

    public Transaction newTrans() {
        return getZk().transaction();
    }

    private WriteLock aquireLock(String lockPath) {
        WriteLock lock = new WriteLock(getZk(), lockPath, Ids.OPEN_ACL_UNSAFE);
        locks.add(lock);
        try {
            while (!lock.lock()) {
                sleep(500);
            }
        }
        catch (ConnectionLossException e) {
            LOGGER.warn("Zk lost connection while aquiring lock will try again in 500 ms");
            forceReconnect();
            sleep(500);
            aquireLock(lockPath);
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        return lock;
    }

    public static interface SynchronizedOperationCallback<R> {
        R perform() throws InterruptedException, KeeperException;
    }

    public void shutdown() {
        try {
            stopAllLockAttempts();
            zookeeper.close();
        }
        catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    private void stopAllLockAttempts() {
        for (WriteLock lock : locks) {
            lock.close();
        }
    }

    public boolean isShutdown() {
        return !getZk().getState().isAlive();
    }

    public static class ZKTransWrapper {
        private Transaction transaction;
        private boolean commited;
        private ZKClient zk;
        private int commitRetry = 1;
        private static final int MAX_RETRY_COUNT = 3;

        public ZKTransWrapper(ZKClient zk) {
            this.transaction = zk.newTrans();
            this.zk = zk;

        }

        public ZKTransWrapper create(String path) {
            ensureNotCommited();
            transaction.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return this;
        }

        public ZKTransWrapper create(String path, byte[] data) {
            ensureNotCommited();
            transaction.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return this;
        }

        public ZKTransWrapper createEphemeral(String path) {
            ensureNotCommited();
            transaction.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return this;
        }

        private void ensureNotCommited() {
            checkState(!commited, "Cannot invoke this method once we the transaction is commited");
        }

        public ZKTransWrapper delete(String path) {
            transaction.delete(path, -1);
            return this;
        }

        public void commit() throws InterruptedException, KeeperException {
            ensureNotCommited();
            try {
                transaction.commit();
                commited = true;
            }
            catch (ConnectionLossException e) {
                zk.forceReconnect();
                if (commitRetry < MAX_RETRY_COUNT) {
                    LOGGER.error("Connections loss while commiting transcation, retrying  {} of {} after 1 second"
                            + commitRetry, MAX_RETRY_COUNT);
                    Thread.sleep(1000);
                    commitRetry++;
                    zk.getZk();
                    commit();
                }
                else {
                    LOGGER.error("Received connection loss after {} retries, giving up ", commitRetry);
                    Throwables.propagate(e);
                }
            }
            catch (KeeperException e) {
                throw e;
            }
        }

        public ZKTransWrapper deleteRecursive(String path) {
            ensureNotCommited();
            List<String> children;
            children = zk.getChildren(path);
            for (String child : children) {
                deleteRecursive(path + "/" + child);
            }
            delete(path);
            return this;
        }
    }
}
