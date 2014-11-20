package org.mbellani;

import org.mbellani.pool.ZombieException;

public class ZombieObjectFactory extends TestObjectFactory {

    private FailureExpectaion zombieOnValidation = new FailureExpectaion();
    private FailureExpectaion zombieOnDestroy = new FailureExpectaion();
    private FailureExpectaion failValidation = new FailureExpectaion();

    @Override
    public boolean validate(TestObject t) {
        if (zombieOnValidation.fail()) {
            throw new ZombieException("Object is a zombie");

        }
        return !failValidation.fail();
    }

    @Override
    public void destroy(TestObject to) {
        if (zombieOnDestroy.fail()) {
            throw new ZombieException("Object is a zombie");

        }
    }

    public FailureExpectaion zombieOnValidation() {
        return zombieOnValidation;
    }

    public FailureExpectaion zombieOnDestroy() {
        return zombieOnDestroy;
    }

    public FailureExpectaion failValidation() {
        return failValidation;
    }

    public static class FailureExpectaion {
        private int expected;
        private int failed;
        private boolean enabled;
        private boolean always;

        public FailureExpectaion() {

        }

        public boolean fail() {
            return enabled && (always || failed++ < expected);
        }

        public FailureExpectaion always() {
            enabled = true;
            this.always = true;
            return this;
        }

        public FailureExpectaion never() {
            this.always = false;
            expected = -1;
            return this;
        }

        public FailureExpectaion times(int times) {
            enabled = true;
            expected = times;
            return this;
        }

        public int getExpected() {
            return expected;
        }

        public int getFailed() {
            return failed;
        }
    }

}
