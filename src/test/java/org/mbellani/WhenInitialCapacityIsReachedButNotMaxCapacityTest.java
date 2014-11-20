package org.mbellani;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class WhenInitialCapacityIsReachedButNotMaxCapacityTest extends BaseObjectPoolTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.initialCapacity = 5;
        super.maxCapacity = 10;
        super.setUp();
    }

    @Override
    public void exercisePool() {
        for (int i = 0; i < 6; i++) {
            pool.borrow();
        }
    }

    @Test
    public void zookeeper_should_contain_6_objects_in_pool() {
        assertEquals("Object pool should have been grown to 6 objects", 6, zkClient.getChildren(paths.master()).size());
    }

    @Test
    public void zookeeper_should_contain_6_objects_in_used_pool() {
        assertEquals("Used objects have been grown to 6 objects", 6, zkClient.getChildren(paths.used()).size());
    }

    @Test
    public void zookeeper_should_not_have_any_objects_in_unused_pool() {
        assertEquals("There should not have been any used objects since everything is borrowed", 0, zkClient
                .getChildren(paths.unused()).size());
    }

    @Test
    public void object_pool_size_should_be_6() {
        assertEquals("Object pool size should be 6 after the pool growth", 6, pool.getSize());
    }

    @Test
    public void object_pool_should_indicate_6_used_objects() {
        assertEquals("Object pools should indicate that there are 6 objects in use", 6, pool.getUsed());
    }

    @Test
    public void object_pool_should_indicate_0_unused_objects() {
        assertEquals("Object pools should indicate that there are no unused objects", 0, pool.getUnused());
    }

}
