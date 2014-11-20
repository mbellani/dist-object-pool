package org.mbellani;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class WhenBorrowingTokensFromPoolTest extends BaseObjectPoolTest {

	private TestObject borrowedObject;

	@Override
	@Before
	public void setUp() throws Exception{
		super.initialCapacity = 5;
		super.setUp();
	}

	@Test
	public void should_return_non_null_object() {
		Assert.assertNotNull("Borrowed objects should not be null", borrowedObject);
	}

	@Test
	public void zookeeper_should_contain_1_object_under_used_node() {
		Assert.assertEquals(1, zkClient.getChildren(paths.used()).size());
	}

	@Test
	public void zookeeper_should_contain_4_objects_under_unused_node() {
		Assert.assertEquals(4, zkClient.getChildren(paths.unused()).size());
	}

	@Test
	public void should_not_impact_the_size_of_base_pool_node_in_zookeeper() {
		Assert.assertEquals(5, zkClient.getChildren(paths.master()).size());
	}

	@Override
	public void exercisePool() {
		borrowedObject = pool.borrow();
	}

}
