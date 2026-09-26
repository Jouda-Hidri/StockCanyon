package stockcanyon.consumption;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.leader.AbstractCandidate;
import org.springframework.integration.leader.Context;

/**
 * This instance's candidacy for ingesting the feed.
 *
 * <p>Election itself is Spring Integration's {@code LockRegistryLeaderInitiator} over a
 * {@code JdbcLockRegistry}: a lock row with a TTL, renewed by the holder, taken over by another
 * instance once it expires. That decides who <em>tries</em> to write. What stops a deposed leader
 * that has not noticed yet is the fencing term checked in every write transaction — see
 * {@link stockcanyon.storage.LeadershipRepository}.
 *
 * <p>Both callbacks run on the election thread and must return promptly: it is the thread that
 * renews the lock, and an exception thrown here is treated as losing it.
 */
public class IngestionLeader extends AbstractCandidate {

    private static final Logger log = LoggerFactory.getLogger(IngestionLeader.class);

    public static final String ROLE = "marketdata-ingestion";

    private final QuoteConsumer consumer;
    private final MeterRegistry meters;

    public IngestionLeader(String instanceId, QuoteConsumer consumer, MeterRegistry meters) {
        super(instanceId, ROLE);
        this.consumer = consumer;
        this.meters = meters;
    }

    @Override
    public void onGranted(Context context) {
        log.info("{} granted leadership of {}", getId(), getRole());
        meters.counter("marketdata.leader.transitions", "event", "granted").increment();
        // If the run ends on its own — fenced by a newer term, or failed — give the lock up rather
        // than keep renewing it while nothing ingests.
        consumer.startConsuming(context::yield);
    }

    @Override
    public void onRevoked(Context context) {
        log.warn("{} lost leadership of {}", getId(), getRole());
        meters.counter("marketdata.leader.transitions", "event", "revoked").increment();
        consumer.stopConsuming(false);
    }
}
