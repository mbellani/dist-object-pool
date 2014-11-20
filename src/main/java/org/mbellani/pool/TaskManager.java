package org.mbellani.pool;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskManager<T> {

    private List<ObjectPoolTask<T>> tasks = newArrayList();
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskManager.class);

    public TaskManager(ZKObjectPool<T> pool) {
        tasks.add(new CrashDetector<T>(pool));
        tasks.add(new PoolCleanup<T>(pool));
        tasks.add(new ZombieMgr<T>(pool));
    }

    public void start() {
        for (ObjectPoolTask<T> task : tasks) {
            LOGGER.info("Starting {}", task.getClass().getName());
            task.start();
        }
    }

    public void shutdown() {
        for (ObjectPoolTask<T> task : tasks) {
            LOGGER.info("Stopping {}", task.getClass().getName());
            task.shutdown();
        }
    }

}
