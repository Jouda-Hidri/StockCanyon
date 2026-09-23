package stockcanyon.consumption;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import stockcanyon.Isin;
import stockcanyon.Quote;

class QuoteBufferTest {

    private static final Isin AAPL = Isin.of("US0378331005");
    private static final Isin MSFT = Isin.of("US5949181045");
    private static final Instant T0 = Instant.parse("2026-09-23T10:00:00Z");

    @Test
    @DisplayName("coalescing keeps only the newest quote per instrument")
    void coalescingKeepsTheNewest() {
        List<Quote> batch = List.of(
                quote(AAPL, 1, T0, "100"),
                quote(AAPL, 2, T0.plusMillis(10), "101"),
                quote(MSFT, 3, T0.plusMillis(20), "200"),
                quote(AAPL, 4, T0.plusMillis(30), "102"));

        Collection<Quote> coalesced = QuoteBuffer.coalesceLatest(batch);

        assertThat(coalesced).hasSize(2);
        assertThat(coalesced).extracting(q -> q.isin().value())
                .containsExactlyInAnyOrder(AAPL.value(), MSFT.value());
        assertThat(coalesced).filteredOn(q -> q.isin().equals(AAPL))
                .singleElement()
                .extracting(Quote::sequence).isEqualTo(4L);
    }

    /**
     * The behaviour that makes an uneven feed cheap. One instrument printing a thousand times
     * while another prints once must cost two writes, not a thousand and one.
     */
    @Test
    @DisplayName("a hot instrument collapses to one write regardless of its tick rate")
    void hotInstrumentCollapses() {
        List<Quote> batch = new java.util.ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            batch.add(quote(AAPL, i, T0.plusMillis(i), "100." + i));
        }
        batch.add(quote(MSFT, 5_000, T0.plusSeconds(2), "200"));

        Collection<Quote> coalesced = QuoteBuffer.coalesceLatest(batch);

        assertThat(coalesced).hasSize(2);
        assertThat(batch).hasSize(1_001);
    }

    /**
     * Coalescing must be decided by event time, not by position in the batch. A quote replayed
     * after a recovery arrives later but is older, and letting it win would move the top of book
     * backwards.
     */
    @Test
    @DisplayName("an out-of-order replay does not win the coalesce")
    void outOfOrderReplayLoses() {
        List<Quote> batch = List.of(
                quote(AAPL, 10, T0.plusSeconds(5), "105"),
                quote(AAPL, 4, T0.plusSeconds(1), "101"));

        Collection<Quote> coalesced = QuoteBuffer.coalesceLatest(batch);

        assertThat(coalesced).singleElement().extracting(Quote::sequence).isEqualTo(10L);
    }

    @Test
    @DisplayName("deduplication removes quotes sharing a primary key")
    void dedupesByKey() {
        Quote original = quote(AAPL, 1, T0, "100");
        List<Quote> batch = List.of(original, quote(AAPL, 1, T0, "100"), quote(AAPL, 2, T0, "101"));

        assertThat(QuoteBuffer.dedupeByKey(batch)).hasSize(2);
    }

    @Test
    @DisplayName("a batch with no duplicates is returned untouched")
    void dedupeIsAllocationFreeWhenNothingToDo() {
        List<Quote> batch = List.of(quote(AAPL, 1, T0, "100"), quote(MSFT, 2, T0, "200"));

        assertThat(QuoteBuffer.dedupeByKey(batch)).isSameAs(batch);
    }

    @Test
    @DisplayName("draining waits for the first quote and then takes what is there")
    void drainsUpToTheCap() throws Exception {
        QuoteBuffer buffer = new QuoteBuffer(10);
        buffer.put(quote(AAPL, 1, T0, "100"));
        buffer.put(quote(MSFT, 2, T0, "200"));

        assertThat(buffer.drain(10, Duration.ofMillis(50))).hasSize(2);
        assertThat(buffer.drain(10, Duration.ofMillis(10))).isEmpty();
    }

    /**
     * The backpressure contract. A full buffer must block the producer rather than grow or drop:
     * blocking is what eventually stops the socket being read and slows the exchange down, and it
     * is the only one of the three options that loses nothing.
     */
    @Test
    @DisplayName("a full buffer blocks the producer until space is freed")
    void fullBufferBlocksTheProducer() throws Exception {
        QuoteBuffer buffer = new QuoteBuffer(2);
        buffer.put(quote(AAPL, 1, T0, "100"));
        buffer.put(quote(AAPL, 2, T0.plusMillis(1), "101"));

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            entered.countDown();
            try {
                buffer.put(quote(AAPL, 3, T0.plusMillis(2), "102"));
                completed.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();

        assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(completed.await(200, TimeUnit.MILLISECONDS))
                .as("the producer must still be blocked on a full buffer")
                .isFalse();

        buffer.drain(1, Duration.ofMillis(10));

        assertThat(completed.await(2, TimeUnit.SECONDS))
                .as("the producer must proceed once space is freed")
                .isTrue();
        assertThat(buffer.blockedCount()).isEqualTo(1);
        producer.join(TimeUnit.SECONDS.toMillis(2));
    }

    private static Quote quote(Isin isin, long sequence, Instant eventTime, String price) {
        BigDecimal mid = new BigDecimal(price);
        return new Quote(
                isin,
                sequence,
                mid.subtract(new BigDecimal("0.01")),
                mid.add(new BigDecimal("0.01")),
                BigDecimal.valueOf(100),
                BigDecimal.valueOf(100),
                "USD",
                eventTime,
                eventTime.plusMillis(5));
    }
}
