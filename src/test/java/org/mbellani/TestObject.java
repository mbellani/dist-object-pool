package org.mbellani;

import com.google.common.base.Objects;

public class TestObject {
    private String testData;
    private boolean zombie;

    public TestObject(String testData) {
        this.testData = testData;
    }

    public String getTestData() {
        return testData;
    }

    public void setTestData(String testData) {
        this.testData = testData;
    }

    public boolean isZombie() {
        return zombie;
    }

    public void setZombie(boolean zombie) {
        this.zombie = zombie;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TestObject other = (TestObject) obj;
        return Objects.equal(this.testData, other.testData);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("testData", testData).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.testData);
    }

}