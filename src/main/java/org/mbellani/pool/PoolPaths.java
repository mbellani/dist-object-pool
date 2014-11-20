package org.mbellani.pool;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;

public class PoolPaths {

    private static enum KEYS {
        BASE,
        MASTER,
        USED,
        UNUSED,
        PARTICIPANTS,
        SHUTDOWN_LOCK,
        CRASH_CLEANUP_LOCK,
        EVICTION_LOCK,
        ZOMBIES,
        ZOMBIES_LOCK
    }

    private Map<KEYS, String> paths = Maps.newLinkedHashMap();
    private String base;

    public PoolPaths(String base) {
        this.base = "/" + base;
        paths.put(KEYS.BASE, this.base);
        paths.put(KEYS.MASTER, this.base + "/master");
        paths.put(KEYS.UNUSED, this.base + "/unused");
        paths.put(KEYS.USED, this.base + "/used");
        paths.put(KEYS.PARTICIPANTS, this.base + "/participants");
        paths.put(KEYS.CRASH_CLEANUP_LOCK, this.base + "/crash-cleanup-lock");
        paths.put(KEYS.SHUTDOWN_LOCK, this.base + "/shutdown-lock");
        paths.put(KEYS.EVICTION_LOCK, this.base + "/eviction-lock");
        paths.put(KEYS.ZOMBIES, this.base + "/zombies");
        paths.put(KEYS.ZOMBIES_LOCK, this.base + "/zombies-lock");
    }

    public String base() {
        return base;
    }

    public String master() {
        return paths.get(KEYS.MASTER);
    }

    public String unused() {
        return paths.get(KEYS.UNUSED);
    }

    public String used() {
        return paths.get(KEYS.USED);
    }

    public String participants() {
        return paths.get(KEYS.PARTICIPANTS);
    }

    public String shutdownLock() {
        return paths.get(KEYS.SHUTDOWN_LOCK);
    }

    public String crashCleanupLock() {
        return paths.get(KEYS.CRASH_CLEANUP_LOCK);
    }

    public String evictionLock() {
        return paths.get(KEYS.EVICTION_LOCK);
    }

    public String zombies() {
        return paths.get(KEYS.ZOMBIES);
    }

    public String zombiesLock() {
        return paths.get(KEYS.ZOMBIES_LOCK);
    }

    public Collection<String> all() {
        return paths.values();
    }

}
