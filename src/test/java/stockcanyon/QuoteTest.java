package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** One malformed quote must be rejected at the boundary, not fail every batch it lands in. */
class QuoteTest {

    private static final Isin AAPL = Isin.of("US0378331005");
    private static final Instant T = Instant.parse("2026-09-26T10:00:00Z");

    @Test
    @DisplayName("a currency that could break the CSV or the column is rejected")
    void rejectsBadCurrencies() {
        for (String currency : new String[] {"U,S", "U\"S", "ßxx", "US", "USDX", "U S"}) {
            assertThatThrownBy(() -> quote(currency, BigDecimal.ONE, BigDecimal.ONE))
                    .as(currency).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(quote("usd", BigDecimal.ONE, BigDecimal.ONE).currency()).isEqualTo("USD");
    }

    @Test
    @DisplayName("prices and sizes beyond the column precision are rejected")
    void rejectsOutOfRange() {
        assertThatThrownBy(() -> quote("USD", new BigDecimal("1000000000000"), BigDecimal.ONE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> quote("USD", BigDecimal.ONE, new BigDecimal("1E16")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(quote("USD", new BigDecimal("999999999999.99999999"), new BigDecimal("9999999999999999.9"))).isNotNull();
    }

    private static Quote quote(String currency, BigDecimal price, BigDecimal size) {
        return new Quote(AAPL, 1, price, price, size, size, currency, T, T);
    }
}
