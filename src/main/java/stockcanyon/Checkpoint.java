package stockcanyon;

import java.time.Instant;

/**
 * How far the feed has been consumed and durably stored.
 *
 * <p>Written in the same transaction as the quotes it covers, so it can never point past data
 * that is not on disk. A lagging checkpoint costs a replay; a leading one loses data silently.
 *
 * <p>{@code sequence} is carried even though the exchange only accepts a timestamp: several quotes
 * can share an instant, so resuming re-delivers some. The sequence is how they are recognised.
 */
public record Checkpoint(Instant eventTime, long sequence, Instant updatedAt) {

    /** Cold start: subscribe from the current moment. */
    public static Checkpoint none() {
        return new Checkpoint(null, -1, null);
    }

    public boolean isPresent() {
        return eventTime != null;
    }

    /** Value for {@code checkpoint_timestamp}, or null to subscribe from now. */
    public String toQueryParameter() {
        return eventTime == null ? null : eventTime.toString();
    }
}
