package org.mbellani;

import org.mbellani.pool.ObjectFactory;

public class TestObjectFactory implements ObjectFactory<TestObject> {

    private int sequence;
    private Counts counts = new Counts();

    public TestObject create() {
        counts.created++;
        return new TestObject((sequence++) + String.valueOf(Math.random()));
    }

    public void destroy(TestObject to) {
        counts.destroyed++;
    }

    public byte[] serialize(TestObject t) {
        return t.toString().getBytes();
    }

    public TestObject deserialize(byte[] bytes) {
        return new TestObject(new String(bytes));
    }

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
