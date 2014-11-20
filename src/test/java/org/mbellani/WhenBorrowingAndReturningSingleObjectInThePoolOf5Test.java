package org.mbellani;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class WhenBorrowingAndReturningSingleObjectInThePoolOf5Test extends BaseObjectPoolTest {

	@Override
	@Before
	public void setUp() throws Exception {
		super.initialCapacity = 5;
		super.setUp();
	}

	@Test
	public void zookeeper_should_not_contain_any_children_under_used_node() {
		assertEquals(
		    "used node in zookeeper should not contain any children after all the objects have been returned to the pool",
		    0, zkClient.getChildren(paths.used()).size());
	}

	@Test
	public void zookeeper_should_contain_5_children_under_unused_node() {
		assertEquals("unused node in zookeeper should contain 5 children since all the objects have been returned to pool",
		    5, zkClient.getChildren(paths.unused()).size());
	}

	@Test
	public void zookeeper_should_contain_5_children_under_master_node() {
		assertEquals("base object pool size should be unchanged ", 5, zkClient.getChildren(paths.master()).size());
	}

	@Test
	public void object_pool_should_indicate_0_used_objects() {
		assertEquals("Object pool should not have any objects in use", 0, pool.getUsed());
	}

	@Test
	public void object_pool_should_indicate_5_unused_objects() {
		assertEquals("Object pool should have 5 unused objects", 5, pool.getUnused());
	}

	@Test
	public void object_pool_size_should_remain_unchanged() {
		assertEquals("Object size should remian unchanged", 5, pool.getSize());
	}

	@Override
	public void exercisePool() {
		TestObject borrowedObject = pool.borrow();
		pool.returnObject(borrowedObject);
	}
}
