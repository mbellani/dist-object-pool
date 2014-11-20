package org.mbellani.pool;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.mbellani.zk.ZKClient;
import org.mbellani.zk.ZKClient.SynchronizedOperationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZombieMgr<T> implements ObjectPoolTask<T>, Runnable {

    private ZKObjectPool<T> pool;
    private ZKClient zk;
    private PoolPaths paths;
    private volatile boolean shutdown;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZombieMgr.class);
    private ScheduledThreadPoolExecutor scheduler;

    public ZombieMgr(ZKObjectPool<T> pool) {
        this.pool = pool;
        this.zk = pool.getZk();
        this.paths = pool.getPaths();
        scheduler = new ScheduledThreadPoolExecutor(1);
    }

    @Override
    public void start() {
        Config c = pool.getConfig();
        scheduler.scheduleAtFixedRate(this, c.getZombieDetectionIntrvl(), c.getZombieDetectionIntrvl(),
                c.getIntrvlUnit());
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for clean up scheduler to shutdown", e);
        }
    }

    @Override
    public void run() {
        if (shutdown) {
            return;
        }
        int zombies = pool.getZombies();
        if (zombies > 0) {
            LOGGER.info("Found {} zombies starting cleanup ", zombies);
            int cleaned = zk.doSynchronized(paths.zombiesLock(), new SynchronizedOperationCallback<Integer>() {
                public Integer perform() {
                    return cleanup();
                }
            });
            if (cleaned > 0) {
                LOGGER.info("Cleaned {} zombies", cleaned);
            }

        }
    }

    private Integer cleanup() {
        int cleaned = 0;
        for (String zombie : pool.getZombieNodes()) {
            T obj = pool.getData(zombie);
            try {
                if (pool.isValid(obj)) {
                    pool.unzombie(zombie);
                }
                else {
                    pool.drop(zombie);
                }
                cleaned++;
            }
            catch (ZombieException e) {
                break;
            }
            catch (Exception e) {
                LOGGER.error("Error cleaning zombies ", e);
            }
        }
        return cleaned;
    }
}
