package stockcanyon.consumption;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import stockcanyon.Isin;
import stockcanyon.Quote;

class QuoteBatchTest {

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

        Collection<Quote> coalesced = QuoteBatch.coalesceLatest(batch);

        assertThat(coalesced).hasSize(2);
        assertThat(coalesced).extracting(q -> q.isin().value())
                .containsExactlyInAnyOrder(AAPL.value(), MSFT.value());
        assertThat(coalesced).filteredOn(q -> q.isin().equals(AAPL))
                .singleElement()
                .extracting(Quote::sequence).isEqualTo(4L);
    }

    /** An uneven feed must cost two writes, not a thousand and one. */
    @Test
    @DisplayName("a hot instrument collapses to one write regardless of its tick rate")
    void hotInstrumentCollapses() {
        List<Quote> batch = new java.util.ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            batch.add(quote(AAPL, i, T0.plusMillis(i), "100." + i));
        }
        batch.add(quote(MSFT, 5_000, T0.plusSeconds(2), "200"));

        Collection<Quote> coalesced = QuoteBatch.coalesceLatest(batch);

        assertThat(coalesced).hasSize(2);
        assertThat(batch).hasSize(1_001);
    }

    /** Decided by event time, not batch position: a replayed quote arrives later but is older. */
    @Test
    @DisplayName("an out-of-order replay does not win the coalesce")
    void outOfOrderReplayLoses() {
        List<Quote> batch = List.of(
                quote(AAPL, 10, T0.plusSeconds(5), "105"),
                quote(AAPL, 4, T0.plusSeconds(1), "101"));

        Collection<Quote> coalesced = QuoteBatch.coalesceLatest(batch);

        assertThat(coalesced).singleElement().extracting(Quote::sequence).isEqualTo(10L);
    }

    @Test
    @DisplayName("deduplication removes quotes sharing a primary key")
    void dedupesByKey() {
        Quote original = quote(AAPL, 1, T0, "100");
        List<Quote> batch = List.of(original, quote(AAPL, 1, T0, "100"), quote(AAPL, 2, T0, "101"));

        assertThat(QuoteBatch.dedupeByKey(batch)).hasSize(2);
    }

    @Test
    @DisplayName("a batch with no duplicates is returned untouched")
    void dedupeIsAllocationFreeWhenNothingToDo() {
        List<Quote> batch = List.of(quote(AAPL, 1, T0, "100"), quote(MSFT, 2, T0, "200"));

        assertThat(QuoteBatch.dedupeByKey(batch)).isSameAs(batch);
    }

    @Test
    @DisplayName("the high-water mark is the newest quote, not the last one delivered")
    void highWaterMarkIsByEventTime() {
        List<Quote> batch = List.of(
                quote(AAPL, 10, T0.plusSeconds(5), "105"),
                quote(MSFT, 11, T0.plusSeconds(1), "201"));

        assertThat(QuoteBatch.highWaterMark(batch).sequence()).isEqualTo(10L);
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
