package org.mbellani;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

public class WhenPoolCapacityHasBeenReached extends BaseObjectPoolTest {
    private TestObject borrowedObject;

    @Override
    @Before
    public void setUp() throws Exception {
        super.initialCapacity = 5;
        super.maxCapacity = 5;
        super.setUp();
    }

    @Test
    public void borrowed_object_should_be_null() {
        assertNull("Pool should not return any objects after it's been exhaused. ", borrowedObject);
    }

    @Test
    public void object_pool_size_should_be_equal_to_max_capacity() {
        assertEquals("Pool Size should be qual to max capacity", super.maxCapacity, pool.getSize());
    }

    @Test
    public void zookeeper_should_not_have_more_nodes_under_used_pool_than_pool_capacity() {
        assertEquals("Used node in zookeeper should not contain more objects than max capacity.", super.maxCapacity,
                zkClient.getChildren(paths.used()).size());
    }

    @Test
    public void zookeeper_should_not_have_any_nodes_under_unused_pool() {
        assertEquals("Unused node in zookeeper should have no children.", 0, zkClient.getChildren(paths.unused())
                .size());
    }

    @Test
    public void object_pool_should_report_all_objects_in_used() {
        assertEquals("Object pool should have all objects in used.", super.maxCapacity, pool.getUsed());
    }

    @Test
    public void object_pool_should_not_report_any_unused_objects() {
        assertEquals("Object pool should not have any unused objects", 0, pool.getUnused());
    }

    private void exhaustPool() {
        for (int i = 0; i < super.maxCapacity; i++) {
            pool.borrow();
        }
    }

    @Override
    public void exercisePool() {
        exhaustPool();
        borrowedObject = pool.borrow();
    }

}
