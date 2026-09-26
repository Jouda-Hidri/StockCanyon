package stockcanyon;

import org.testcontainers.containers.GenericContainer;

/** One Redis for the distribution tests, started once, like {@link SharedPostgres}. */
final class SharedRedis {

    static final GenericContainer<?> INSTANCE;

    static {
        INSTANCE = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
        INSTANCE.start();
    }

    static int port() {
        return INSTANCE.getMappedPort(6379);
    }

    private SharedRedis() {
    }
}
