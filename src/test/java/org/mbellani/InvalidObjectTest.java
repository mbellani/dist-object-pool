package org.mbellani;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class InvalidObjectTest {

    public static class WhenFactoryMarksAnObjectInvalid extends BaseObjectPoolTest {
        @Before
        public void setUp() throws Exception {
            super.factory = new InvalidatingObjFactory();
            super.initialCapacity = 2;
            super.maxCapacity = 5;
            super.setUp();
        }

        @Test
        public void should_destroy_2_objects() {
            assertThat(factory.getCounts().destroyed, is(2));
        }

        @Test
        public void should_create_4_objects() {
            assertThat(factory.getCounts().created, is(4));
        }

        @Test
        public void should_have_2_objects_in_the_pool() {
            assertThat(pool.getSize(), is(2));
        }

        @Test
        public void should_have_2_used_objects() {
            assertThat(pool.getUsed(), is(2));
        }

        @Override
        public void exercisePool() {
            for (int i = 0; i < 2; i++) {
                pool.borrow();
            }
        }

    }

    public static class WhenNoObjectsAreInvalid extends BaseObjectPoolTest {
        @Before
        public void setUp() throws Exception {
            InvalidatingObjFactory factory = new InvalidatingObjFactory();
            factory.expected(0);
            super.factory = factory;
            super.initialCapacity = 2;
            super.maxCapacity = 5;
            super.setUp();
        }

        @Test
        public void should_not_destroy_any_objects() {
            assertThat(factory.getCounts().destroyed, is(0));
        }

        @Test
        public void should_create_2_objects() {
            assertThat(factory.getCounts().created, is(2));
        }

        @Test
        public void should_have_2_objects_in_the_pool() {
            assertThat(pool.getSize(), is(2));
        }

        @Test
        public void should_have_2_used_objects() {
            assertThat(pool.getUsed(), is(2));
        }

        @Override
        public void exercisePool() {
            for (int i = 0; i < 2; i++) {
                pool.borrow();
            }
        }

    }

}
