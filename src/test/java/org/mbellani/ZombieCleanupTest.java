package org.mbellani;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mbellani.pool.Config;
import org.mbellani.pool.ZKObjectPool;
import org.mbellani.zk.ZKClient;

@RunWith(Enclosed.class)
public class ZombieCleanupTest {

    private static final int SIZE = 5;
    private static final int EXPECTED_ZOMBIES = 2;
    private static final int INTRVL = 5;

    private static Config config() {
        return new Config.Builder("test").initSize(SIZE).size(SIZE).zkConnectString(ZkServer.connectString())
                .zombieDetectionIntrvl(INTRVL).build();
    }

    public static class WhenZombieRevalidationSucceeds {
        private ZKClient zk;
        private ZKObjectPool<TestObject> pool;

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            zk = new ZKClient(ZkServer.connectString());
            configurePool();
            pool.returnObject(pool.borrow());
            Thread.sleep(TimeUnit.SECONDS.toMillis(INTRVL + 5));
        }

        private void configurePool() {
            ZombieObjectFactory factory = new ZombieObjectFactory();
            factory.zombieOnValidation().times(EXPECTED_ZOMBIES);
            pool = new ZKObjectPool<TestObject>(config());
            pool.setFactory(factory);
            pool.initialize();
        }

        @Test
        public void should_not_have_any_zombies() {
            assertThat(pool.getZombies(), is(0));
        }

        @Test
        public void should_not_increase_pool_size() {
            assertThat(pool.getSize(), is(SIZE));
        }

        @Test
        public void should_not_have_used_objects() {
            assertThat(pool.getUsed(), is(0));
        }

        @Test
        public void should_have_all_unused() {
            assertThat(pool.getUnused(), is(SIZE));
        }

        @Test
        public void should_match_pool_sizes_with_zk() {
            pool.borrow();
            assertThat(pool.getZombies(), is(zk.getStat(pool.getPaths().zombies()).getNumChildren()));
            assertThat(pool.getUsed(), is(zk.getStat(pool.getPaths().used()).getNumChildren()));
            assertThat(pool.getSize(), is(zk.getStat(pool.getPaths().master()).getNumChildren()));
            assertThat(pool.getUnused(), is(zk.getStat(pool.getPaths().unused()).getNumChildren()));
        }

        @After
        public void after() {
            pool.shutdown();
        }

    }

    public static class WhenObjectMarkedZombieAfterValidation {
        private ZKClient zk;
        private ZKObjectPool<TestObject> pool;

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            zk = new ZKClient(ZkServer.connectString());
            configurePool();
            TestObject obj = pool.borrow();
            assertThat(obj, is(nullValue()));
            Thread.sleep(TimeUnit.SECONDS.toMillis(INTRVL + 5));
        }

        private void configurePool() {
            ZombieObjectFactory factory = new ZombieObjectFactory();
            factory.zombieOnValidation().always();
            pool = new ZKObjectPool<TestObject>(config());
            pool.setFactory(factory);
            pool.initialize();
        }

        @Test
        public void should_have_zombies() {
            assertThat(pool.getZombies(), is(SIZE));
        }

        @Test
        public void should_not_increase_pool_size() {
            assertThat(pool.getSize(), is(SIZE));
        }

        @Test
        public void should_have_marked_all_used() {
            assertThat(pool.getUsed(), is(SIZE));
        }

        @Test
        public void should_not_have_unused_objects() {
            assertThat(pool.getUnused(), is(0));
        }

        @Test
        public void should_match_pool_sizes_with_zk() {
            pool.borrow();
            assertThat(pool.getZombies(), is(zk.getStat(pool.getPaths().zombies()).getNumChildren()));
            assertThat(pool.getUsed(), is(zk.getStat(pool.getPaths().used()).getNumChildren()));
            assertThat(pool.getSize(), is(zk.getStat(pool.getPaths().master()).getNumChildren()));
            assertThat(pool.getUnused(), is(zk.getStat(pool.getPaths().unused()).getNumChildren()));
        }

        @After
        public void after() {
            pool.shutdown();
        }
    }

    public static class WhenObjectIsInvalidatedFromZombieState {
        private ZKClient zk;
        private ZKObjectPool<TestObject> pool;
        private ZombieObjectFactory factory = new ZombieObjectFactory();

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            zk = new ZKClient(ZkServer.connectString());
            configurePool();
            TestObject obj = pool.borrow();
            assertThat(obj, is(nullValue()));
            factory.zombieOnValidation().never();
            factory.failValidation().times(2);
            Thread.sleep(TimeUnit.SECONDS.toMillis(INTRVL + 5));
        }

        @After
        public void after() {
            pool.shutdown();
        }

        private void configurePool() {
            factory = new ZombieObjectFactory();
            factory.zombieOnValidation().always();
            pool = new ZKObjectPool<TestObject>(config());
            pool.setFactory(factory);
            pool.initialize();
        }

        @Test
        public void should_reduce_zombie_count() {
            assertThat(pool.getZombies(), is(0));
        }

        @Test
        public void should_reduce_the_pool_size() {
            assertThat(pool.getSize(), is(3));
        }

        @Test
        public void should_not_have_any_used_objects() {
            assertThat(pool.getUsed(), is(0));
        }

        @Test
        public void should_have_unused_objects() {
            assertThat(pool.getUnused(), is(3));
        }

        @Test
        public void should_match_pool_sizes_with_zk() {
            pool.borrow();
            assertThat(pool.getZombies(), is(zk.getStat(pool.getPaths().zombies()).getNumChildren()));
            assertThat(pool.getUsed(), is(zk.getStat(pool.getPaths().used()).getNumChildren()));
            assertThat(pool.getSize(), is(zk.getStat(pool.getPaths().master()).getNumChildren()));
            assertThat(pool.getUnused(), is(zk.getStat(pool.getPaths().unused()).getNumChildren()));
        }

    }

}
