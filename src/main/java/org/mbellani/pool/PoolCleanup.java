package org.mbellani.pool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.mbellani.zk.ZKClient;
import org.mbellani.zk.ZKClient.SynchronizedOperationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

public class PoolCleanup<T> implements ObjectPoolTask<T> {

    private ZKObjectPool<?> pool;
    private ScheduledThreadPoolExecutor scheduler;
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolCleanup.class);

    public PoolCleanup(ZKObjectPool<T> pool) {
        this.pool = pool;
        scheduler = new ScheduledThreadPoolExecutor(1);
    }

    public void start() {
        Config cfg = pool.getConfig();
        if (cfg.evictionEnabled()) {
            scheduler.scheduleAtFixedRate(new PoolCleanupExecutor(pool), cfg.getEvictIntrvl(), cfg.getEvictIntrvl(),
                    cfg.getIntrvlUnit());
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for clean up scheduler to shutdown", e);
        }
    }

    private static class PoolCleanupExecutor implements Runnable, SynchronizedOperationCallback<Integer> {
        private Config cfg;
        @SuppressWarnings("rawtypes")
        private ZKObjectPool pool;
        private ZKClient zk;
        private PoolPaths paths;

        private Ordering<Stat> sorter = new Ordering<Stat>() {
            public int compare(Stat left, Stat right) {
                return Longs.compare(left.getMtime(), right.getMtime());
            }
        };

        public PoolCleanupExecutor(ZKObjectPool<?> pool) {
            this.pool = pool;
            this.zk = pool.getZk();
            this.cfg = pool.getConfig();
            this.paths = pool.getPaths();
        }

        public void run() {
            zk.doSynchronized(paths.evictionLock(), this);
        }

        @SuppressWarnings("unchecked")
        public Integer perform() throws InterruptedException, KeeperException {
            Map<Stat, String> stats = zk.getChildrenStats(paths.unused());
            List<Stat> sortedStats = sorter.immutableSortedCopy(stats.keySet());
            int objsToTest = sortedStats.size() < cfg.getNumTestsPerEviction() ? sortedStats.size() : cfg
                    .getNumTestsPerEviction();
            LOGGER.debug("Starting Eviction, found {} unused objects ", sortedStats.size());
            int evicted = 0;
            try {
                for (int i = 0; i < objsToTest; i++) {
                    Stat nodeStat = sortedStats.get(i);
                    if (shouldEvict(nodeStat)) {
                        String unusedNode = stats.get(nodeStat);
                        Object unusedObj = pool.borrowSpecific(unusedNode);
                        if (unusedObj != null && pool.invalidate(unusedObj)) {
                            evicted++;
                        }
                    }
                }
            }
            catch (Exception e) {
                LOGGER.error("Error running cleanup ", e);
            }
            if (evicted > 0) {
                LOGGER.info("Evicted {} objects ", evicted);
            }
            return evicted;
        }

        private boolean shouldEvict(Stat stat) {
            Config cfg = pool.getConfig();
            long idleTime = cfg.getIntrvlUnit().convert(System.currentTimeMillis() - stat.getMtime(),
                    TimeUnit.MILLISECONDS);
            return idleTime >= cfg.getMaxIdleIntrvl();
        }
    }
}
