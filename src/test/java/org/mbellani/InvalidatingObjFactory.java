package org.mbellani;

public class InvalidatingObjFactory extends TestObjectFactory {
    private int pending = 2;
    private int done;

    public int getPending() {
        return pending;
    }

    public void expected(int invaliObjCount) {
        this.pending = invaliObjCount;
    }

    @Override
    public boolean validate(TestObject t) {
        boolean ret = false;
        ret = done < pending ? false : true;
        done++;
        return ret;
    }
}
