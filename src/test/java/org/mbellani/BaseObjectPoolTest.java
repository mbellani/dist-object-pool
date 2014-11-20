package org.mbellani;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.mbellani.pool.Config;
import org.mbellani.pool.PoolPaths;
import org.mbellani.pool.ZKObjectPool;
import org.mbellani.zk.ZKClient;

public abstract class BaseObjectPoolTest {

    private static final int DEFAULT_INITIAL_CAPACITY = 0;
    private static final int DEFAULT_MAX_CAPACITY = 5;
    protected int initialCapacity = DEFAULT_INITIAL_CAPACITY;
    protected int maxCapacity = DEFAULT_MAX_CAPACITY;
    protected String poolName = "test-pool";
    protected ZKObjectPool<TestObject> pool;
    protected PoolPaths paths = new PoolPaths(poolName);
    protected ZKClient zkClient;
    protected Config config;
    protected TestObjectFactory factory = new TestObjectFactory();

    @Before
    public void setUp() throws Exception {
        ZkServer.start();
        config = new Config.Builder(poolName).initSize(initialCapacity).size(maxCapacity)
                .zkConnectString(ZkServer.connectString()).build();
        zkClient = new ZKClient(ZkServer.connectString());
        pool = new ZKObjectPool<TestObject>(config);
        pool.setFactory(factory);
        pool.initialize();
        exercisePool();
    }

    @After
    public void destroyPool() {
        pool.shutdown();
        Assert.assertFalse("I did not expect " + pool.getConfig().getName()
                + " in the zoo after the destroy has been invoked.", zkClient.exists(paths.base()));
        zkClient.shutdown();
    }

    public abstract void exercisePool();

}
