package stockcanyon.consumption;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
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
