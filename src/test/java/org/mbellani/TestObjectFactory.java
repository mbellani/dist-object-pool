package org.mbellani;

import org.mbellani.pool.ObjectFactory;

public class TestObjectFactory implements ObjectFactory<TestObject> {

    private int sequence;
    private Counts counts = new Counts();

    @Override
    public TestObject create() {
        counts.created++;
        return new TestObject((sequence++) + String.valueOf(Math.random()));
    }

    @Override
    public void destroy(TestObject to) {
        counts.destroyed++;
    }

    @Override
    public byte[] serialize(TestObject t) {
        return t.toString().getBytes();
    }

    @Override
    public TestObject deserialize(byte[] bytes) {
        return new TestObject(new String(bytes));
    }

    @Override
    public boolean validate(TestObject t) {
        return true;
    }

    public Counts getCounts() {
        return counts;
    }

    public static class Counts {
        public int created;
        public int destroyed;
    }

}
