package stockcanyon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * One PostgreSQL container, started once and never explicitly stopped, with a database per test
 * class.
 *
 * <p>{@code @Testcontainers} is not used: it stops the container when the class finishes, while the
 * cached Spring context closes later from a shutdown hook — so the consumer's final flush ran
 * against a database that had already gone. Leaving it running inverts that ordering.
 *
 * <p>Separate databases because two test classes each run a full pipeline against their own
 * simulated exchange, and sharing tables interleaves two unrelated sequence streams.
 */
final class SharedPostgres {

    static final PostgreSQLContainer<?> INSTANCE;

    static {
        INSTANCE = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("marketdata")
                .withUsername("marketdata")
                .withPassword("marketdata")
                // Logical decoding, so tests can read what the outbox put in the WAL.
                .withCommand("postgres", "-c", "wal_level=logical");
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
