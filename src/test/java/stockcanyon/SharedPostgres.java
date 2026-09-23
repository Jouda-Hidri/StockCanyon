package stockcanyon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One PostgreSQL container for the market data tests, with a separate database per test class.
 *
 * <p>The usual {@code @Testcontainers} / {@code @Container} pair is deliberately not used, because
 * its teardown ordering is wrong for a service that writes on shutdown. The JUnit extension stops
 * the container when the test class finishes, whereas the Spring context — cached and shared
 * between test classes — is closed later, from a JVM shutdown hook. The ingestion service's final
 * flush therefore ran against a database that had already gone, blocked until the connection
 * timeout, and reported a failure that said nothing about the code under test.
 *
 * <p>Leaving the container running inverts that: the context closes first and drains cleanly, and
 * Testcontainers' own reaper removes the container once the JVM exits.
 *
 * <p>Each test class gets its own database rather than sharing one. Every class that enables the
 * module runs a full ingestion pipeline against its own simulated exchange, and two of those
 * writing to the same tables interleave two unrelated sequence streams — which makes the
 * contiguity check in {@code MarketDataRecoveryTest} fail for a reason that has nothing to do with
 * recovery. Isolating the schemas keeps each test measuring only its own feed.
 */
final class SharedPostgres {

    static final PostgreSQLContainer<?> INSTANCE;

    static {
        INSTANCE = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("marketdata")
                .withUsername("marketdata")
                .withPassword("marketdata");
        INSTANCE.start();
    }

    /** Creates {@code name} if it does not exist and returns a JDBC URL pointing at it. */
    static String jdbcUrlFor(String name) {
        try (Connection connection = DriverManager.getConnection(
                INSTANCE.getJdbcUrl(), INSTANCE.getUsername(), INSTANCE.getPassword());
                Statement statement = connection.createStatement()) {
            boolean exists = statement.executeQuery(
                    "SELECT 1 FROM pg_database WHERE datname = '" + name + "'").next();
            if (!exists) {
                // CREATE DATABASE cannot run inside a transaction or take a bind parameter, hence
                // the literal. The name is a compile-time constant from the test, not input.
                statement.execute("CREATE DATABASE " + name);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create the test database " + name, e);
        }
        return INSTANCE.getJdbcUrl().replaceFirst("/" + INSTANCE.getDatabaseName() + "(\\?|$)", "/" + name + "$1");
    }

    private SharedPostgres() {
    }
}
