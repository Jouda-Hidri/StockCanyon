package stockcanyon;

/**
 * An ISIN, validated on construction including the check digit.
 *
 * <p>Shape alone is not enough: a transposed pair still matches {@code [A-Z]{2}[A-Z0-9]{9}[0-9]}
 * but names a different instrument, or none. Caught here it is a 400; uncaught it is a silent
 * miss, or quotes filed against the wrong security.
 */
public record Isin(String value) implements Comparable<Isin> {

    public static final int LENGTH = 12;

    public Isin {
        if (value == null) {
            throw new IllegalArgumentException("ISIN must not be null");
        }
        value = value.trim().toUpperCase();
        if (value.length() != LENGTH) {
            throw new IllegalArgumentException(
                    "ISIN must be " + LENGTH + " characters, got " + value.length() + ": " + value);
        }
        if (!value.chars().allMatch(Isin::isAlphanumeric)) {
            throw new IllegalArgumentException("ISIN must be alphanumeric: " + value);
        }
        if (!Character.isLetter(value.charAt(0)) || !Character.isLetter(value.charAt(1))) {
            throw new IllegalArgumentException("ISIN must start with a 2-letter country code: " + value);
        }
        int expected = checkDigit(value.substring(0, LENGTH - 1));
        int actual = value.charAt(LENGTH - 1) - '0';
        if (!Character.isDigit(value.charAt(LENGTH - 1)) || expected != actual) {
            throw new IllegalArgumentException(
                    "ISIN check digit is " + value.charAt(LENGTH - 1) + " but should be " + expected
                            + ": " + value);
        }
    }

    public static Isin of(String value) {
        return new Isin(value);
    }

    /** Whether {@code value} is a well-formed ISIN. */
    public static boolean isValid(String value) {
        try {
            new Isin(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * ISO 6166 check digit for the first eleven characters.
     *
     * <p>Letters expand to two digits (A=10 ... Z=35), then Luhn over the result. The expansion
     * happens first, so a letter occupies two positions and can straddle the alternation — which is
     * why this cannot be done in one pass over the original characters.
     */
    static int checkDigit(String body) {
        StringBuilder digits = new StringBuilder(body.length() * 2);
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (Character.isDigit(c)) {
                digits.append(c);
            } else {
                digits.append(c - 'A' + 10);
            }
        }
        int sum = 0;
        boolean doubling = true;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int digit = digits.charAt(i) - '0';
            if (doubling) {
                digit *= 2;
                if (digit > 9) {
                    digit -= 9;
                }
            }
            sum += digit;
            doubling = !doubling;
        }
        return (10 - (sum % 10)) % 10;
    }


    @Override
    public int compareTo(Isin other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }

    private static boolean isAlphanumeric(int c) {
        return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z');
    }
}
