package org.mbellani;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mbellani.pool.Config;
import org.mbellani.pool.PoolPaths;
import org.mbellani.pool.ZKObjectPool;
import org.mbellani.zk.ZKClient;

import com.google.common.collect.Lists;

@RunWith(Enclosed.class)
public class PoolInitializationTest {

    public static class WhenInitialCapacityNotSet extends BaseObjectPoolTest {

        @Override
        public void exercisePool() {
            // Pool already initialized do nothing.
        }

        @Test
        public void should_have_all_paths_in_zk() {
            for (String path : paths.all()) {
                assertThat(zkClient.exists(path), is(true));
            }
        }

        @Test
        public void should_not_have_children_under_master_node() {
            assertThat(zkClient.getChildren(paths.master()).size(), is(0));
        }

        @Test
        public void should_not_have_children_under_used_node() {
            assertThat(zkClient.getChildren(paths.used()).size(), is(0));
        }

        @Test
        public void should_not_have_children_under_unused_node() {
            assertThat(zkClient.getChildren(paths.unused()).size(), is(0));
        }

        @Test
        public void should_match_unused_size_with_zk() {
            assertThat(pool.getUnused(), is(zkClient.getChildren(paths.unused()).size()));
        }

        @Test
        public void should_match_pool_size_with_zk() {
            assertThat(pool.getSize(), is(zkClient.getChildren(paths.master()).size()));
        }

        @Test
        public void should_match_used_size_with_zk() {
            assertThat(pool.getUsed(), is(zkClient.getChildren(paths.used()).size()));
        }

    }

    public static class WhenInitialCapacityIsSet extends BaseObjectPoolTest {

        @Override
        @Before
        public void setUp() throws Exception {
            initialCapacity = 5;
            super.setUp();
        }

        @Override
        public void exercisePool() {
        }

        @Test
        public void should_have_all_paths_in_zk() {
            for (String path : paths.all()) {
                assertThat(zkClient.exists(path), is(true));
            }
        }

        @Test
        public void should_have_children_under_master_node() {
            assertThat(zkClient.getChildren(paths.master()).size(), is(super.initialCapacity));
        }

        @Test
        public void should_have_children_under_unused_node() {
            assertThat(zkClient.getChildren(paths.unused()).size(), is(super.initialCapacity));
        }

        @Test
        public void should_not_have_children_under_used_node() {
            assertThat(zkClient.getChildren(paths.used()).size(), is(0));
        }

        @Test
        public void should_match_unused_size_with_zk() {
            assertThat(pool.getUnused(), is(zkClient.getChildren(paths.unused()).size()));
        }

        @Test
        public void should_match_pool_size_with_zk() {
            assertThat(pool.getSize(), is(zkClient.getChildren(paths.master()).size()));
        }

        @Test
        public void should_match_used_size_with_zk() {
            assertThat(pool.getUsed(), is(zkClient.getChildren(paths.used()).size()));
        }
    }

    public static class WhenMultipleClientsInitializePool {

        private static final String OBJECT_POOL_NAME = "test";
        private ZKObjectPool<TestObject> pool1;
        private ZKObjectPool<TestObject> pool2;
        private ExecutorService executroService;
        private int initialCapacity = 5;
        private int maxCapacity = 5;
        private PoolPaths paths;
        protected ZKClient zkClient;

        @Before
        public void setup() throws Exception {
            zkClient = new ZKClient(ZkServer.connectString());

            paths = new PoolPaths(OBJECT_POOL_NAME);
            List<Future<?>> futures = Lists.newArrayList();
            pool1 = newPoolInstance();
            pool2 = newPoolInstance();
            executroService = Executors.newFixedThreadPool(2);
            futures.add(executroService.submit(runnableFor(pool1)));
            futures.add(executroService.submit(runnableFor(pool2)));
            for (Future<?> f : futures) {
                f.get();
            }

        }

        private ZKObjectPool<TestObject> newPoolInstance() {
            Config c = new Config.Builder(OBJECT_POOL_NAME).initSize(initialCapacity).size(maxCapacity)
                    .zkConnectString(ZkServer.connectString()).build();
            ZKObjectPool<TestObject> pool = new ZKObjectPool<TestObject>(c);
            pool.setFactory(new TestObjectFactory());
            return pool;
        }

        private Runnable runnableFor(final ZKObjectPool<TestObject> pool) {
            return new Runnable() {
                @Override
                public void run() {
                    pool.initialize();
                }
            };
        }

        @After
        public void tearDown() throws Exception {
            pool1.shutdown();
            pool2.shutdown();
            Assert.assertFalse("I did not expect " + OBJECT_POOL_NAME
                    + " in the zoo after the destroy has been invoked.", zkClient.exists(paths.base()));
            executroService.shutdown();
        }

        @Test
        public void should_not_exceed_initial_capacity() {
            assertThat(pool1.getSize(), is(initialCapacity));
            assertThat(pool2.getSize(), is(initialCapacity));
        }

        @Test
        public void should_have_same_sized_pools() {
            assertSame("Pool1 and Pool2 both should have the same size", pool1.getSize(), pool2.getSize());
        }

        @Test
        public void should_have_children_under_master_node() {
            assertThat(zkClient.getChildren(paths.master()).size(), is(initialCapacity));
        }

        @Test
        public void should_not_have_children_under_used_node() {
            assertThat(zkClient.getChildren(paths.used()).size(), is(0));
        }

        @Test
        public void should_have_children_under_unused_node() {
            assertThat(zkClient.getChildren(paths.unused()).size(), is(initialCapacity));
        }

        @Test
        public void should_match_unused_size_with_zk() {
            assertThat(pool1.getUnused(), is(zkClient.getChildren(paths.unused()).size()));
            assertThat(pool2.getUnused(), is(zkClient.getChildren(paths.unused()).size()));
        }

        @Test
        public void should_match_pool_size_with_zk() {
            assertThat(pool1.getSize(), is(zkClient.getChildren(paths.master()).size()));
            assertThat(pool2.getSize(), is(zkClient.getChildren(paths.master()).size()));
        }

        @Test
        public void should_match_used_size_with_zk() {
            assertEquals("Pool1 should not have any used objects", 0, pool1.getUsed());
            assertEquals("Pool2 should not have any used objects", 0, pool2.getUsed());
        }

    }
}
