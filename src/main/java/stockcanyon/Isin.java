package stockcanyon;

import org.apache.commons.validator.routines.ISINValidator;

/**
 * An ISIN, validated on construction including the check digit.
 *
 * <p>Shape alone is not enough: a transposed pair still matches {@code [A-Z]{2}[A-Z0-9]{9}[0-9]}
 * but names a different instrument, or none. Caught here it is a 400; uncaught it is a silent
 * miss, or quotes filed against the wrong security.
 */
public record Isin(String value) implements Comparable<Isin> {

    /** {@code false}: check the format and the ISO 6166 check digit, not the country registry. */
    private static final ISINValidator VALIDATOR = ISINValidator.getInstance(false);

    public Isin {
        if (value == null) {
            throw new IllegalArgumentException("ISIN must not be null");
        }
        value = value.trim().toUpperCase();
        if (!VALIDATOR.isValid(value)) {
            throw new IllegalArgumentException("Not a valid ISIN: " + value);
        }
    }

    public static Isin of(String value) {
        return new Isin(value);
    }

    /** Whether {@code value} is a well-formed ISIN. */
    public static boolean isValid(String value) {
        return value != null && VALIDATOR.isValid(value.trim().toUpperCase());
    }

    @Override
    public int compareTo(Isin other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
