package stockcanyon.consumption;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import stockcanyon.Isin;
import stockcanyon.Quote;

class IngestQueueTest {

    @Test
    @DisplayName("the high watermark trips exactly once, on the quote that reaches it")
    void tripsOnceAtHigh() throws Exception {
        IngestQueue queue = new IngestQueue(10, 6, 2);
        for (int i = 0; i < 5; i++) {
            assertThat(queue.put(quote(i))).isFalse();
        }
        assertThat(queue.put(quote(5))).as("the sixth reaches high").isTrue();
        assertThat(queue.put(quote(6))).as("already paused: in-flight frames just land").isFalse();
        assertThat(queue.isPaused()).isTrue();
        assertThat(queue.highWatermarkTrips()).isEqualTo(1);
    }

    /** Hysteresis: between the marks the pause holds, or the socket would flap on every batch. */
    @Test
    @DisplayName("the pause holds until the writer drains to the low watermark")
    void resumesOnlyAtLow() throws Exception {
        IngestQueue queue = new IngestQueue(10, 6, 2);
        for (int i = 0; i < 7; i++) {
            queue.put(quote(i));
        }
        queue.drain(new ArrayList<>(), 3, Duration.ZERO);
        queue.afterDrain();
        assertThat(queue.depth()).isEqualTo(4);
        assertThat(queue.isPaused()).as("below high is not enough").isTrue();

        queue.drain(new ArrayList<>(), 2, Duration.ZERO);
        queue.afterDrain();
        assertThat(queue.depth()).isEqualTo(2);
        assertThat(queue.isPaused()).isFalse();
    }

    @Test
    @DisplayName("the connection gate blocks while paused and opens on resume")
    void gateFollowsThePause() throws Exception {
        IngestQueue queue = new IngestQueue(10, 3, 1);
        for (int i = 0; i < 3; i++) {
            queue.put(quote(i));
        }
        CompletableFuture<Void> gate = CompletableFuture.runAsync(() -> {
            try {
                queue.awaitResumed();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(100);
        assertThat(gate).as("reconnecting while paused would refill the queue at once").isNotDone();

        queue.drain(new ArrayList<>(), 2, Duration.ZERO);
        queue.afterDrain();
        gate.get(2, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("drain returns a full batch without waiting, and a partial one after the wait")
    void drainBatching() throws Exception {
        IngestQueue queue = new IngestQueue(100, 90, 10);
        for (int i = 0; i < 5; i++) {
            queue.put(quote(i));
        }
        List<Quote> full = new ArrayList<>();
        queue.drain(full, 3, Duration.ofSeconds(10));
        assertThat(full).hasSize(3);

        List<Quote> partial = new ArrayList<>();
        long started = System.nanoTime();
        queue.drain(partial, 10, Duration.ofMillis(50));
        assertThat(partial).hasSize(2);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("watermarks must be ordered low < high < capacity")
    void validatesWatermarks() {
        assertThatThrownBy(() -> new IngestQueue(10, 10, 2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IngestQueue(10, 5, 5)).isInstanceOf(IllegalArgumentException.class);
    }

    private static Quote quote(long sequence) {
        Instant t = Instant.parse("2026-09-26T10:00:00Z").plusMillis(sequence);
        return new Quote(Isin.of("US0378331005"), sequence, BigDecimal.ONE, BigDecimal.TEN,
                BigDecimal.ONE, BigDecimal.ONE, "USD", t, t);
    }
}
