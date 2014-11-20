package org.mbellani.pool;

@SuppressWarnings("serial")
public class ZombieException extends RuntimeException {

    public ZombieException() {
    }

    public ZombieException(String msg) {
        super(msg);
    }

    public ZombieException(Throwable cause) {
        super(cause);
    }

    public ZombieException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
