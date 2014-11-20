package org.mbellani;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mbellani.ZombieObjectFactory.FailureExpectaion;
import org.mbellani.pool.Config;
import org.mbellani.pool.ZKObjectPool;
import org.mbellani.zk.ZKClient;

@RunWith(Enclosed.class)
public class ZombieTest {
    private static final int SIZE = 5;

    public static class WhenAllMarkedZombiesDuringValidation {
        private ZKClient zk;
        private ZKObjectPool<TestObject> pool;

        @Before
        public void setup() throws Exception {
            ZkServer.start();
            zk = new ZKClient(ZkServer.connectString());
            pool = new ZKObjectPool<TestObject>(config());
            ZombieObjectFactory factory = new ZombieObjectFactory();
            factory.zombieOnValidation().always();
            pool.setFactory(factory);
            pool.initialize();
        }

        @Test
        public void should_return_null_when_borrowed() {
            assertThat(pool.borrow(), is(nullValue()));
        }

        @Test
        public void should_not_reduce_pool_size() {
            pool.borrow();
            assertThat(pool.getSize(), is(SIZE));
        }

        @Test
        public void should_have_all_objects_in_use() {
            pool.borrow();
            assertThat(pool.getUsed(), is(SIZE));

        }

        @Test
        public void should_not_have_unused_objects() {
            pool.borrow();
            assertThat(pool.getUnused(), is(0));
        }

        @Test
        public void should_mark_all_objects_as_zombies() {
            pool.borrow();
            assertThat(pool.getZombies(), is(SIZE));
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
        public void shutdown() {
            pool.shutdown();
        }

    }

    public static class WhenSomeMarkedZombiesDuringValidation {

        private static final int SIZE = 5;
        private ZKClient zk;
        private ZKObjectPool<TestObject> pool;
        private FailureExpectaion expectation;

        @Before
        public void setup() throws Exception {
            ZombieObjectFactory factory = new ZombieObjectFactory();
            expectation = factory.zombieOnValidation().times(2);
            ZkServer.start();
            zk = new ZKClient(ZkServer.connectString());
            pool = new ZKObjectPool<TestObject>(config());
            pool.setFactory(factory);
            pool.initialize();

        }

        @Test
        public void should_return_a_valid_object() {
            assertThat(pool.borrow(), is(notNullValue()));
        }

        @Test
        public void should_not_reduce_pool_size() {
            pool.borrow();
            assertThat(pool.getSize(), is(SIZE));
        }

        @Test
        public void should_have_some_objects_in_use() {
            pool.borrow();
            assertThat(pool.getUsed(), is(SIZE - expectation.getExpected()));

        }

        @Test
        public void should_have_some_unused_objects() {
            pool.borrow();
            assertThat(pool.getUnused(), is(SIZE - pool.getUsed()));
        }

        @Test
        public void should_match_expected_zombies() {
            pool.borrow();
            assertThat(pool.getZombies(), is(expectation.getExpected()));
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
        public void shutdown() {
            pool.shutdown();
        }

    }

    public static class WhenAllMarkedZombiesDuringDestroy {
        private ZKClient zk;
        private ZKObjectPool<TestObject> pool;

        @Before
        public void setup() throws Exception {
            ZombieObjectFactory factory = new ZombieObjectFactory();
            factory.zombieOnDestroy().times(SIZE);
            ZkServer.start();
            zk = new ZKClient(ZkServer.connectString());
            pool = new ZKObjectPool<TestObject>(config());
            pool.setFactory(factory);
            pool.initialize();
            destroyAllObjs();
        }

        private void destroyAllObjs() {
            for (int i = 0; i < pool.getSize(); i++) {
                pool.invalidate(pool.borrow());
            }
        }

        @Test
        public void should_not_reduce_pool_size() {
            assertThat(pool.getSize(), is(SIZE));
        }

        @Test
        public void should_have_all_objects_in_use() {
            assertThat(pool.getUsed(), is(SIZE));

        }

        @Test
        public void should_not_have_unused_objects() {
            assertThat(pool.getUnused(), is(0));
        }

        @Test
        public void should_mark_all_objects_as_zombies() {
            assertThat(pool.getZombies(), is(SIZE));
        }

        @Test
        public void should_match_pool_sizes_with_zk() {
            assertThat(pool.getZombies(), is(zk.getStat(pool.getPaths().zombies()).getNumChildren()));
            assertThat(pool.getUsed(), is(zk.getStat(pool.getPaths().used()).getNumChildren()));
            assertThat(pool.getSize(), is(zk.getStat(pool.getPaths().master()).getNumChildren()));
            assertThat(pool.getUnused(), is(zk.getStat(pool.getPaths().unused()).getNumChildren()));
        }

        @After
        public void shutdown() {
            pool.shutdown();
        }

    }

    public static class WhenSomeMarkedZombiesDuringDestroy {

        private static final int SIZE = 5;
        private ZKClient zk;
        private ZKObjectPool<TestObject> pool;
        private FailureExpectaion expectation;

        @Before
        public void setup() throws Exception {
            ZombieObjectFactory factory = new ZombieObjectFactory();
            expectation = factory.zombieOnDestroy().times(SIZE);
            ZkServer.start();
            zk = new ZKClient(ZkServer.connectString());
            pool = new ZKObjectPool<TestObject>(config());
            pool.setFactory(factory);
            pool.initialize();
            destroyAllObjs();
        }

        private void destroyAllObjs() {
            for (int i = 0; i < expectation.getExpected(); i++) {
                pool.invalidate(pool.borrow());
            }
        }

        @Test
        public void should_not_reduce_pool_size() {
            assertThat(pool.getSize(), is(SIZE));
        }

        @Test
        public void should_have_some_objects_in_use() {
            assertThat(pool.getUsed(), is(expectation.getExpected()));

        }

        @Test
        public void should_have_some_unused_objects() {
            assertThat(pool.getUnused(), is(SIZE - pool.getUsed()));
        }

        @Test
        public void should_match_expected_zombies() {
            assertThat(pool.getZombies(), is(expectation.getExpected()));
        }

        @Test
        public void should_match_pool_sizes_with_zk() {
            assertThat(pool.getZombies(), is(zk.getStat(pool.getPaths().zombies()).getNumChildren()));
            assertThat(pool.getUsed(), is(zk.getStat(pool.getPaths().used()).getNumChildren()));
            assertThat(pool.getSize(), is(zk.getStat(pool.getPaths().master()).getNumChildren()));
            assertThat(pool.getUnused(), is(zk.getStat(pool.getPaths().unused()).getNumChildren()));
        }

        @After
        public void shutdown() {
            pool.shutdown();
        }

    }

    private static Config config() {
        return new Config.Builder("test").initSize(SIZE).size(5).zkConnectString(ZkServer.connectString()).build();
    }

}
