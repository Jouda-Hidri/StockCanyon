package stockcanyon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IsinTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "US0378331005",  // Apple
            "US5949181045",  // Microsoft
            "GB0002634946",  // BAE Systems
            "DE0007164600",  // SAP
            "NL0011821202",  // ING
            "CH0038863350",  // Nestle
            "JP3633400001"}) // Toyota
    @DisplayName("accepts ISINs with a correct check digit")
    void acceptsValidIsins(String value) {
        assertThat(Isin.of(value).value()).isEqualTo(value);
    }

    @Test
    @DisplayName("rejects a wrong check digit")
    void rejectsWrongCheckDigit() {
        // Apple's ISIN with the check digit changed from 5 to 6.
        assertThatThrownBy(() -> Isin.of("US0378331006"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Isin.isValid("US0378331005")).isTrue();
    }

    /** Right length, shape and character classes — only the check digit tells them apart. */
    @Test
    @DisplayName("rejects transposed characters that still look like an ISIN")
    void rejectsTransposition() {
        assertThat(Isin.isValid("US0378331005")).isTrue();
        assertThat(Isin.isValid("US0387331005")).isFalse();
        assertThat(Isin.isValid("US0378313005")).isFalse();
    }

    @Test
    @DisplayName("rejects malformed input")
    void rejectsMalformed() {
        assertThat(Isin.isValid(null)).isFalse();
        assertThat(Isin.isValid("")).isFalse();
        assertThat(Isin.isValid("US037833100")).isFalse();     // too short
        assertThat(Isin.isValid("US03783310055")).isFalse();   // too long
        assertThat(Isin.isValid("0S0378331005")).isFalse();    // country code not alphabetic
        assertThat(Isin.isValid("US03783310_5")).isFalse();    // not alphanumeric
        assertThat(Isin.isValid("US037833100X")).isFalse();    // check digit not numeric
    }

    @Test
    @DisplayName("normalises case and surrounding whitespace")
    void normalises() {
        assertThat(Isin.of("  us0378331005 ").value()).isEqualTo("US0378331005");
    }
}
