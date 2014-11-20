package org.mbellani.pool;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.mbellani.zk.ZKClient;
import org.mbellani.zk.ZKClient.SynchronizedOperationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class CrashDetector<T> implements ObjectPoolTask<T>, Watcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(CrashDetector.class);
    private volatile boolean running;
    private volatile int lastKnownParticipants;
    private ZKClient zkClient;
    private PoolPaths paths;

    public CrashDetector(ZKObjectPool<T> pool) {
        this.zkClient = pool.getZk();
        this.paths = pool.getPaths();
    }

    public void start() {
        running = true;
        lastKnownParticipants = zkClient.getChildren(paths.participants(), this).size();
    }

    public void shutdown() {
        running = false;
    }

    public void process(WatchedEvent e) {
        if (!running || nonParticipantCrashEvent(e) || zkClient.isShutdown()) {
            return;
        }
        int activeParticipants = zkClient.getChildren(paths.participants(), this).size();
        if (activeParticipants < lastKnownParticipants && activeParticipants != 0) {
            LOGGER.info("Participants have dropped to {} from {} initiating cleanup.. ", activeParticipants,
                    lastKnownParticipants);
            zkClient.doSynchronized(paths.crashCleanupLock(), new SynchronizedOperationCallback<Object>() {
                public Object perform() throws InterruptedException, KeeperException {
                    LOGGER.info("Successfully aquired lock to perform cleanup...");
                    cleanup();
                    LOGGER.info("Cleanup finished...");
                    return null;
                }
            });
        }
        lastKnownParticipants = activeParticipants;
    }

    private boolean nonParticipantCrashEvent(WatchedEvent e) {
        if (e == null || e.getPath() == null) {
            return true;
        }
        return ((e.getType() == EventType.NodeDeleted || e.getType() == EventType.None) && e.getPath().equals(
                paths.participants()));
    }

    private void cleanup() {
        List<String> missingNodes = findMissingNodes();
        if (missingNodes != null) {
            restoreMissingNodes(missingNodes);
        }
        else {
            LOGGER.info("No missing nodes found");
        }
    }

    private List<String> findMissingNodes() {
        List<String> missingNodes = null;
        List<String> master = zkClient.getChildren(paths.master());
        List<String> unused = zkClient.getChildren(paths.unused());
        List<String> used = zkClient.getChildren(paths.used());
        LOGGER.info("find missing: master {}, used {}, unused {}",
                new Object[] { master.size(), used.size(), unused.size() });
        if ((used.size() + unused.size()) < master.size()) {
            missingNodes = Lists.newArrayList(master);
            missingNodes.removeAll(used);
            missingNodes.removeAll(unused);
        }
        return missingNodes;
    }

    private void restoreMissingNodes(List<String> missingNodes) {
        for (String missingNode : missingNodes) {
            restoreNode(missingNode);
        }
    }

    private void restoreNode(String missingNode) {
        try {
            // extra guard in case we caught a node in transition
            if (!zkClient.exists(paths.used() + "/" + missingNode)) {
                zkClient.create(paths.unused() + "/" + missingNode);
            }
            else {
                LOGGER.warn("Node {} was reported missing but exists under the used node", missingNode);
            }
        }
        catch (KeeperException.NodeExistsException e) {
            // do nothing, we might have caught a node in transition not due to
            // crash.
            LOGGER.info("Node {} already exists under unused nodes, was caught in transition", missingNode);
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
    }

}
