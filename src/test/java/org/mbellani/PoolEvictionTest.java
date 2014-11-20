package org.mbellani;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mbellani.pool.Config;
import org.mbellani.pool.ZKObjectPool;

@RunWith(Enclosed.class)
public class PoolEvictionTest {
    private static final String NAME = "test";
    private static final int INITIAL_CAPACITY = 5;
    private static final int MAX_CAPACITY = 10;
    private static final int TESTS_PER_EVICTION = 3;
    public static final Long EVIC_INTRVL = 3L;
    private static final Long IDLE_INTRVL = 3L;
    public static final TimeUnit INTRVL_UNIT = TimeUnit.SECONDS;

    private static List<ZKObjectPool<TestObject>> create(int numPools, Config cfg) {
        List<ZKObjectPool<TestObject>> ret = newArrayList();
        for (int i = 0; i < numPools; i++) {
            ZKObjectPool<TestObject> pool = new ZKObjectPool<TestObject>(cfg);
            pool.setFactory(new TestObjectFactory());
            ret.add(pool);
        }
        return ret;
    }

    private static Config cfg() {
        return new Config.Builder(NAME).initSize(INITIAL_CAPACITY).size(MAX_CAPACITY)
                .evictIntrvl(EVIC_INTRVL).intrvlUnit(INTRVL_UNIT).zkConnectString(ZkServer.connectString())
                .numTestsPerEviction(TESTS_PER_EVICTION).maxIdleIntrvl(IDLE_INTRVL).build();
    }

    public static class WhenNoObjectsAreInUse {
        private ZKObjectPool<TestObject> pool;
        private TestObjectFactory factory = new TestObjectFactory();

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            pool = create(1, cfg()).get(0);
            pool.setFactory(factory);
            pool.initialize();
            // WAIT FOR CLEANUP TO OCCUR
            Thread.sleep(INTRVL_UNIT.toMillis(EVIC_INTRVL * 2));
        }

        @Test
        public void should_destroy_objects_exceeding_idle_time_limit() {
            assertThat(factory.getCounts().destroyed, is(INITIAL_CAPACITY - pool.getUnused()));
        }

        @Test
        public void should_not_evict_unused_objects() {
            assertThat(pool.getUnused(), is(INITIAL_CAPACITY - factory.getCounts().destroyed));
        }

        @Test
        public void should_shrink_pool_size() {
            assertThat(pool.getSize(), is(pool.getUnused()));
        }

        @After
        public void after() {
            pool.shutdown();
        }

    }

    public static class WhenSomeObjectsAreInUse {
        private ZKObjectPool<TestObject> pool;
        private TestObjectFactory factory = new TestObjectFactory();

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            pool = create(1, cfg()).get(0);
            pool.setFactory(factory);
            pool.initialize();
            pool.borrow();
            pool.borrow();
            // WAIT FOR CLEANUP TO OCCUR
            Thread.sleep(6000);

        }

        @Test
        public void should_destroy_objects_exceeding_idle_time_limit() {
            assertThat(factory.getCounts().destroyed, is(INITIAL_CAPACITY - pool.getUsed()));
        }

        @Test
        public void should_not_have_any_unused_objects() {
            assertThat(pool.getUnused(), is(0));
        }

        @Test
        public void should_leave_used_objects_in_tact() {
            assertThat(pool.getUsed(), is(INITIAL_CAPACITY - factory.getCounts().destroyed));
        }

        @Test
        public void should_shrink_pool_size() {
            assertThat(pool.getSize(), is(pool.getUsed()));
        }

        @After
        public void after() {
            pool.shutdown();
        }

    }

    public static class WhenCleaningUpWithMultipleClients {
        private List<ZKObjectPool<TestObject>> pools;
        private int totalDestroyed = 0;

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            pools = create(2, cfg());
            for (ZKObjectPool<TestObject> pool : pools) {
                pool.setFactory(new TestObjectFactory());
                pool.initialize();
                pool.borrow();
            }
            Thread.sleep(6000);
            for (ZKObjectPool<TestObject> pool : pools) {
                totalDestroyed += ((TestObjectFactory) pool.getFactory()).getCounts().destroyed;
            }

        }

        @Test
        public void should_destroy_objects_exceeding_idle_time_limit() {
            assertThat(totalDestroyed, is(TESTS_PER_EVICTION));
        }

        @Test
        public void should_not_have_any_unused_objects() {
            for (ZKObjectPool<TestObject> pool : pools) {
                assertThat(pool.getUnused(), is(0));
            }
        }

        @Test
        public void should_shrink_pool_size() {
            for (ZKObjectPool<TestObject> pool : pools) {
                assertThat(pool.getSize(), is(INITIAL_CAPACITY - TESTS_PER_EVICTION));
            }
        }

        @After
        public void after() {
            for (ZKObjectPool<TestObject> pool : pools) {
                pool.shutdown();
            }
        }

    }

}
