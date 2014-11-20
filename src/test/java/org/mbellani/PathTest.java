package org.mbellani;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;
import org.mbellani.pool.PoolPaths;

public class PathTest {

    private static final String BASE = "base-path";
    private static final String BASE_PATH = "/base-path";
    private static final String EXPECTED_MASTER_PATH = BASE_PATH + "/master";
    private static final String EXPECTED_USED_PATH = BASE_PATH + "/used";
    private static final String EXPECTED_UNUSED_PATH = BASE_PATH + "/unused";
    private static final String EXPECTED_PARTICIPANTS_PATH = BASE_PATH + "/participants";
    private static final String EXPECTED_SHUTDOWN_LOCK_PATH = BASE_PATH + "/shutdown-lock";
    private static final String EXPECTED_ZOMBIES_PATH = BASE_PATH + "/zombies";
    private static final String EXPECTED_EVICTION_LOCK_PATH = BASE_PATH + "/eviction-lock";

    private PoolPaths paths = new PoolPaths(BASE);

    @Test
    public void should_have_correct_base_path() {
        assertThat(paths.base(), is(equalTo(BASE_PATH)));
    }

    @Test
    public void should_have_correct_master_path() {
        assertThat(paths.master(), is(equalTo(EXPECTED_MASTER_PATH)));
    }

    @Test
    public void should_have_correct_unused_path() {
        assertThat(paths.unused(), is(equalTo(EXPECTED_UNUSED_PATH)));
    }

    @Test
    public void should_have_correct_used_path() {
        assertThat(paths.used(), is(equalTo(EXPECTED_USED_PATH)));
    }

    @Test
    public void should_have_correct_participants_path() {
        assertThat(paths.participants(), is(equalTo(EXPECTED_PARTICIPANTS_PATH)));
    }

    @Test
    public void should_have_correct_shutdown_lock_path() {
        assertThat(paths.shutdownLock(), is(equalTo(EXPECTED_SHUTDOWN_LOCK_PATH)));
    }

    @Test
    public void should_have_correct_zombies_path() {
        assertThat(paths.zombies(), is(equalTo(EXPECTED_ZOMBIES_PATH)));
    }

    @Test
    public void should_have_correct_eviction_lock_path() {
        assertThat(paths.evictionLock(), is(equalTo(EXPECTED_EVICTION_LOCK_PATH)));
    }
}
