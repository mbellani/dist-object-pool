package org.mbellani.pool;

public interface ObjectFactory<T> {
    T create();

    void destroy(T t) throws ZombieException;

    boolean validate(T t) throws ZombieException;

    byte[] serialize(T t);

    T deserialize(byte[] bytes);

}
