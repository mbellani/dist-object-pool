package org.mbellani.pool;

import java.util.List;

public interface ObjectPool<T> {

    int getSize();

    void shutdown();

    void setFactory(ObjectFactory<T> factory);

    int getUnused();

    int getUsed();

    int getZombies();

    T borrow();

    Config getConfig();

    List<String> getParticipants();

    void returnObject(T object);

    boolean invalidate(T object);

}