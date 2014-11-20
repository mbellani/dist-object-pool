package org.mbellani.pool;

public interface ObjectPoolTask<T> {

    void start();

    void shutdown();

}
