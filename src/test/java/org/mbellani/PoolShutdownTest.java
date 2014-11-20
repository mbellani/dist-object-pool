package org.mbellani;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mbellani.pool.Config;
import org.mbellani.pool.PoolPaths;
import org.mbellani.pool.ZKObjectPool;
import org.mbellani.zk.ZKClient;

@RunWith(Enclosed.class)
public class PoolShutdownTest {
    private static final String NAME = "test";
    private static final int INITIAL_CAPACITY = 5;
    private static final int MAX_CAPACITY = 10;
    private static final int TOTAL_PARTICIPATNS = 3;
    private static final PoolPaths PATHS = new PoolPaths(NAME);

    private static Config cfg() {
        return new Config.Builder(NAME).initSize(INITIAL_CAPACITY).size(MAX_CAPACITY)
                .zkConnectString(ZkServer.connectString()).build();
    }

    private static List<ZKObjectPool<TestObject>> create(int numPools) {
        List<ZKObjectPool<TestObject>> ret = newArrayList();
        for (int i = 0; i < numPools; i++) {
            ZKObjectPool<TestObject> pool = new ZKObjectPool<TestObject>(cfg());
            pool.setFactory(new TestObjectFactory());
            ret.add(pool);
        }
        return ret;
    }

    private static List<ZKObjectPool<TestObject>> createWithFactory(int numPools, TestObjectFactory factory) {
        List<ZKObjectPool<TestObject>> ret = newArrayList();
        for (int i = 0; i < numPools; i++) {
            ZKObjectPool<TestObject> pool = new ZKObjectPool<TestObject>(cfg());
            pool.setFactory(factory);
            ret.add(pool);
        }
        return ret;
    }

    public static class WhenAllObjectsAreReturnedToPool extends BaseObjectPoolTest {
        @Override
        @Before
        public void setUp() throws Exception {
            super.initialCapacity = 5;
            super.setUp();

        }

        @Override
        public void exercisePool() {
            pool.shutdown();
        }

        @Test
        public void zk_should_not_have_pool_node() {
            assertThat(zkClient.exists(paths.base()), is(false));
        }

        @Test
        public void should_have_created_5_objects() {
            assertThat(((TestObjectFactory) pool.getFactory()).getCounts().created, is(5));
        }

        @Test
        public void should_have_destroyed_5_objects() {
            assertThat(((TestObjectFactory) pool.getFactory()).getCounts().destroyed, is(5));
        }

        @Override
        @After
        public void destroyPool() {
            // DO NOTHING SINCE THAT'S WHAT WE'RE TESTING HERE.
        }

    }

    public static class WhenSomeObjectsAreUsed extends BaseObjectPoolTest {
        @Override
        @Before
        public void setUp() throws Exception {
            super.initialCapacity = 5;
            super.setUp();

        }

        @Override
        public void exercisePool() {
            for (int i = 0; i < 3; i++) {
                pool.borrow();
            }
            pool.shutdown();
        }

        @Test
        public void zk_should_not_have_pool_node() {
            assertThat(zkClient.exists(paths.base()), is(false));
        }

        @Test
        public void should_have_created_5_objects() {
            assertThat(((TestObjectFactory) pool.getFactory()).getCounts().created, is(5));
        }

        @Test
        public void should_have_destroyed_5_objects() {
            assertThat(((TestObjectFactory) pool.getFactory()).getCounts().destroyed, is(5));
        }

        @Override
        @After
        public void destroyPool() {
            // DO NOTHING SINCE THAT'S WHAT WE'RE TESTING HERE.
        }

    }

    public static class WhenThereAreNOObjectsInPool extends BaseObjectPoolTest {
        @Override
        @Before
        public void setUp() throws Exception {
            super.initialCapacity = 0;
            super.setUp();

        }

        @Override
        public void exercisePool() {
            pool.shutdown();
        }

        @Test
        public void zk_should_not_have_pool_node() {
            assertThat(zkClient.exists(paths.base()), is(false));
        }

        @Test
        public void should_not_have_created_any_objects() {
            assertThat(((TestObjectFactory) pool.getFactory()).getCounts().created, is(0));
        }

        @Test
        public void should_not_have_destroyed_any_objects() {
            assertThat(((TestObjectFactory) pool.getFactory()).getCounts().destroyed, is(0));
        }

        @Override
        @After
        public void destroyPool() {
            // DO NOTHING SINCE THAT'S WHAT WE'RE TESTING HERE.
        }

    }

    public static class WhenOneOfTheParticipantsShutdown {

        private ZKClient zk;
        List<ZKObjectPool<TestObject>> participants;

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            zk = getZk();
            participants = create(TOTAL_PARTICIPATNS);
            for (ZKObjectPool<TestObject> pool : participants) {
                pool.initialize();
            }
            assertThat(zk.getChildren(PATHS.participants()).size(), is(participants.size()));
            shutdownOne();
        }

        private ZKClient getZk() throws IOException, InterruptedException {
            zk = new ZKClient(ZkServer.connectString());
            return zk;
        }

        private void shutdownOne() {
            participants.remove(0).shutdown();
        }

        @Test
        public void should_have_1_less_participant() {
            assertThat(zk.getChildren(PATHS.participants()).size(), is(TOTAL_PARTICIPATNS - 1));
        }

        @Test
        public void should_have_correct_unused_objects() {
            assertThat(zk.getChildren(PATHS.unused()).size(), is(INITIAL_CAPACITY));
        }

        @Test
        public void should_have_correct_num_of_objects_in_master_list() {
            assertThat(zk.getChildren(PATHS.master()).size(), is(INITIAL_CAPACITY));
        }

        @Test
        public void should_not_remove_pool_nodes() {
            for (String path : PATHS.all()) {
                assertThat(path + " should have been there", zk.exists(path), is(true));
            }
        }

        @After
        public void tearDown() {
            for (ZKObjectPool<TestObject> pool : participants) {
                pool.shutdown();
            }
        }

    }

    public static class WhenAllClientsShutdownConcurrently {

        private List<ZKObjectPool<TestObject>> pools;
        private ZKClient zk;
        private PoolPaths paths;
        private ExecutorService executor = Executors.newFixedThreadPool(2);
        private List<Future<?>> futures = newArrayList();

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            zk = getZk();
            pools = createWithFactory(TOTAL_PARTICIPATNS, new TestObjectFactory());
            for (ZKObjectPool<TestObject> pool : pools) {
                pool.initialize();
            }
            paths = pools.get(0).getPaths();
            assertThat(zk.getChildren(PATHS.participants()).size(), is(pools.size()));
            for (final ZKObjectPool<TestObject> pool : pools) {
                futures.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        pool.shutdown();
                    }
                }));
            }
            awaitShutdown();

        }

        private void awaitShutdown() throws Exception {
            for (Future<?> f : futures) {
                f.get();
            }
        }

        private ZKClient getZk() throws IOException, InterruptedException {
            zk = new ZKClient(ZkServer.connectString());
            return zk;
        }

        @Test
        public void zk_should_not_have_pool_node() {
            assertThat(zk.exists(paths.base()), is(false));
        }

        @Test
        public void should_have_created_5_objects() {
            for (ZKObjectPool<TestObject> pool : pools) {
                assertThat(((TestObjectFactory) pool.getFactory()).getCounts().created, is(INITIAL_CAPACITY));
            }

        }

        @Test
        public void should_have_destroyed_5_objects() {
            for (ZKObjectPool<TestObject> pool : pools) {
                assertThat(((TestObjectFactory) pool.getFactory()).getCounts().destroyed, is(INITIAL_CAPACITY));
            }
        }
    }

}
