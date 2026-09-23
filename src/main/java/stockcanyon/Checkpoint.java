package stockcanyon;

import java.time.Instant;

/**
 * How far the feed has been consumed and durably stored.
 *
 * <p>This is the whole of the gap-free guarantee. The checkpoint is written in the same
 * transaction as the quotes it covers, so it can never point past data that is not on disk; on
 * resume the service asks the exchange to start again from here. The invariant to protect is
 * one-directional: a checkpoint that lags reality costs a replay, whereas a checkpoint that leads
 * it loses data silently. Every ambiguous case in this service is resolved toward the replay.
 *
 * <p>{@code sequence} is carried alongside the timestamp even though the exchange's
 * {@code checkpoint_timestamp} parameter only accepts the latter. The exchange can only rewind to
 * an instant, and several quotes may share one instant, so the resumed stream necessarily
 * re-delivers some messages the service already holds. The sequence is what lets it recognise and
 * drop them.
 *
 * @param eventTime exchange timestamp of the last quote durably stored
 * @param sequence exchange sequence number of that quote
 * @param updatedAt when this service committed it, for operator visibility only
 */
public record Checkpoint(Instant eventTime, long sequence, Instant updatedAt) {

    /** The starting state: no checkpoint, so the feed subscribes from the current moment. */
    public static Checkpoint none() {
        return new Checkpoint(null, -1, null);
    }

    public boolean isPresent() {
        return eventTime != null;
    }

    /**
     * The value for the exchange's {@code checkpoint_timestamp} parameter, or null to subscribe
     * from now.
     */
    public String toQueryParameter() {
        return eventTime == null ? null : eventTime.toString();
    }
}
