package com.csg.airtel.aaa4j.domain.service;

/**
 * Pure text helpers that turn a raw exception message into the two fields an
 * operator actually needs to identify an error: a <em>code</em> and a <em>reason</em>.
 *
 * <p>Both are deliberately <em>low cardinality</em>. A raw message such as
 * {@code "ORA-00001: unique constraint (AAA.PK_SESSION) violated on id 88213"}
 * is unique per occurrence — used as a metric label it would create a new
 * Prometheus time series every time it happened. These helpers reduce it to a
 * stable pair that groups every occurrence of the same fault together:
 *
 * <pre>
 *   code   = "ORA-00001"
 *   reason = "unique constraint ? violated on id #"
 * </pre>
 *
 * <p>The unredacted message is still kept — once per distinct signature — as the
 * {@code sampleMessage} on the {@link ErrorCatalog} entry, so nothing is lost:
 * the metric groups, the sample explains.
 *
 * <p><b>Cost.</b> Every method is a single bounded pass over at most
 * {@value #MAX_REASON_LEN} characters of output, allocating one
 * {@link StringBuilder}. There is no regex, no {@code split}, and no backtracking.
 * Nothing here runs unless an exception has already been constructed, which is
 * itself orders of magnitude more expensive.
 */
final class ErrorSignatures {

    /** Value used when a throwable exposes no usable code. */
    static final String NO_CODE = "none";

    /** Value used when a throwable carries no message at all. */
    static final String NO_REASON = "(no message)";

    /** Reason templates are truncated to this many characters. */
    static final int MAX_REASON_LEN = 96;

    /** Raw sample messages are kept, but never beyond this length. */
    static final int MAX_SAMPLE_LEN = 240;

    /** Only this many leading characters are scanned when looking for a vendor code. */
    private static final int MAX_CODE_SCAN = 64;

    /** A vendor code longer than this is treated as noise, not a code. */
    private static final int MAX_CODE_LEN = 24;

    /** Tokens at least this long that mix letters and digits are treated as identifiers. */
    private static final int IDENTIFIER_MIN_LEN = 8;

    /** At most this many wrapper prefixes ({@code "com.foo.Bar: "}) are stripped. */
    private static final int MAX_PREFIX_STRIPS = 3;

    private ErrorSignatures() {
    }

    /**
     * Extracts the vendor error code that a message leads with.
     *
     * <p>Recognises the {@code LETTERS-DIGITS} form used by virtually every database and
     * telco stack — {@code ORA-01017}, {@code ORA-12541}, {@code ERR-503} — and looks for
     * it only at the <em>head</em> of the message, skipping past any wrapper class prefixes
     * a reactive driver may have prepended:
     *
     * <pre>
     *   "ORA-12541: TNS:no listener"                         -&gt; ORA-12541
     *   "io.vertx.core.VertxException: ORA-12541: TNS:..."   -&gt; ORA-12541
     *   "the dog rested-42"                                  -&gt; null
     * </pre>
     *
     * <p>Anchoring to the head matters: a dash-digit pair buried in prose is not a code,
     * and treating it as one would split a single fault across several catalog rows.
     *
     * @param message a raw exception message, may be {@code null}
     * @return the code (e.g. {@code "ORA-01017"}), or {@code null} if the message has none
     */
    static String codeFromMessage(String message) {
        if (message == null) {
            return null;
        }
        int len = message.length();
        int pos = skipSpaces(message, 0, len);
        for (int pass = 0; pass < MAX_PREFIX_STRIPS && pos < len; pass++) {
            int codeEnd = readCodeAt(message, pos, len);
            if (codeEnd > pos) {
                return message.substring(pos, codeEnd);
            }
            int afterPrefix = skipClassPrefix(message, pos, len);
            if (afterPrefix == pos) {
                return null;
            }
            pos = afterPrefix;
        }
        return null;
    }

    /**
     * Reads a {@code LETTERS-DIGITS} code starting exactly at {@code pos}.
     *
     * @return the exclusive end index of the code, or {@code pos} if there is not one there
     */
    private static int readCodeAt(String message, int pos, int len) {
        int i = pos;
        while (i < len && (isAsciiLetter(message.charAt(i)) || message.charAt(i) == '_')) {
            i++;
        }
        if (i == pos || i >= len || message.charAt(i) != '-') {
            return pos;
        }
        i++;
        int digitsStart = i;
        while (i < len && isAsciiDigit(message.charAt(i))) {
            i++;
        }
        if (i == digitsStart || (i - pos) > MAX_CODE_LEN) {
            return pos;
        }
        // A code is a standalone token: it must end the message or be followed by a
        // delimiter, so "rested-42nd" is not mistaken for one.
        if (i < len && isTokenChar(message.charAt(i))) {
            return pos;
        }
        return i;
    }

    /**
     * Skips a leading fully-qualified class name followed by {@code ": "}, as prepended by
     * wrapping frameworks. Returns {@code pos} unchanged when there is no such prefix.
     */
    private static int skipClassPrefix(String message, int pos, int len) {
        boolean hasDot = false;
        int i = pos;
        int scanLimit = Math.min(len, pos + MAX_CODE_SCAN);
        while (i < scanLimit) {
            char c = message.charAt(i);
            if (c == ':') {
                break;
            }
            if (isSpace(c)) {
                return pos;
            }
            if (c == '.') {
                hasDot = true;
            }
            i++;
        }
        if (i >= scanLimit || i == pos || !hasDot) {
            return pos;
        }
        int next = i + 1;
        if (next >= len || !isSpace(message.charAt(next))) {
            return pos;
        }
        return skipSpaces(message, next, len);
    }

    /**
     * Reduces a raw message to a stable, human-readable reason template.
     *
     * <p>Applied in order:
     * <ol>
     *   <li>wrapper prefixes are dropped — {@code "io.vertx.core.VertxException: "},
     *       {@code "ORA-01017: "} — because the class is already the {@code error}
     *       field and the code is already the {@code code} field;</li>
     *   <li>runs of digits (ids, ports, timestamps, IP addresses) collapse to {@code #};</li>
     *   <li>quoted and bracketed literals (column names, table names, values)
     *       collapse to {@code ?};</li>
     *   <li>long mixed letter/digit tokens (UUIDs, hex, session ids) collapse to {@code #};</li>
     *   <li>whitespace runs collapse to a single space;</li>
     *   <li>the result is truncated to {@value #MAX_REASON_LEN} characters.</li>
     * </ol>
     *
     * <p>Parenthesised text is deliberately <em>kept</em>. Databases put the identifier of
     * the violated object there — {@code "unique constraint (AAA.PK_SESSION) violated"} —
     * which is both naturally low cardinality and the single most useful word in the
     * message. Any digits inside it are still masked by the rules above.
     *
     * @param message a raw exception message, may be {@code null}
     * @return a bounded reason template, never {@code null}
     */
    static String normalizeReason(String message) {
        if (message == null) {
            return NO_REASON;
        }
        int len = message.length();
        int start = skipSpaces(message, 0, len);
        if (start >= len) {
            return NO_REASON;
        }
        start = stripWrapperPrefixes(message, start, len);
        if (start >= len) {
            return NO_REASON;
        }

        StringBuilder out = new StringBuilder(MAX_REASON_LEN + 4);
        int i = start;
        boolean truncated = false;

        while (i < len) {
            if (out.length() >= MAX_REASON_LEN) {
                truncated = true;
                break;
            }
            char c = message.charAt(i);

            if (isSpace(c)) {
                i = skipSpaces(message, i, len);
                if (out.length() > 0 && out.charAt(out.length() - 1) != ' ') {
                    out.append(' ');
                }
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                i = skipQuoted(message, i, len, c);
                appendPlaceholder(out, '?');
                continue;
            }
            if (c == '[' || c == '{') {
                i = skipBracketed(message, i, len, c);
                appendPlaceholder(out, '?');
                continue;
            }
            if (isTokenStart(c)) {
                int end = i;
                while (end < len && isTokenChar(message.charAt(end))) {
                    end++;
                }
                if (looksVariable(message, i, end)) {
                    appendPlaceholder(out, '#');
                } else {
                    // Clamp to the remaining budget: a single very long token must not
                    // push the label past MAX_REASON_LEN.
                    int copyEnd = Math.min(end, i + (MAX_REASON_LEN - out.length()));
                    out.append(message, i, copyEnd);
                    if (copyEnd < end) {
                        truncated = true;
                    }
                }
                i = end;
                continue;
            }
            out.append(c);
            i++;
        }

        trimTrailing(out);
        if (out.length() == 0) {
            return NO_REASON;
        }
        if (truncated) {
            out.append("...");
        }
        return out.toString();
    }

    /**
     * Returns the raw message capped at {@value #MAX_SAMPLE_LEN} characters with
     * newlines flattened, for display as the one concrete example of a signature.
     *
     * @param message a raw exception message, may be {@code null}
     * @return a display-safe sample, never {@code null}
     */
    static String sampleOf(String message) {
        if (message == null || message.isEmpty()) {
            return NO_REASON;
        }
        int len = Math.min(message.length(), MAX_SAMPLE_LEN);
        StringBuilder out = new StringBuilder(len + 3);
        boolean lastSpace = false;
        for (int i = 0; i < len; i++) {
            char c = message.charAt(i);
            if (isSpace(c)) {
                if (!lastSpace && out.length() > 0) {
                    out.append(' ');
                }
                lastSpace = true;
                continue;
            }
            lastSpace = false;
            out.append(c);
        }
        trimTrailing(out);
        if (message.length() > MAX_SAMPLE_LEN) {
            out.append("...");
        }
        return out.length() == 0 ? NO_REASON : out.toString();
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Skips leading {@code "<something>: "} prefixes that add no information:
     * fully-qualified class names and vendor codes. Conservative on purpose — a
     * prefix is only dropped when it is unambiguously one of those two forms, so
     * an ordinary message like {@code "user: not found"} is left intact.
     */
    private static int stripWrapperPrefixes(String message, int from, int len) {
        int start = from;
        for (int pass = 0; pass < MAX_PREFIX_STRIPS; pass++) {
            int colon = -1;
            boolean hasDot = false;
            boolean hasDashDigit = false;
            boolean sawSpace = false;
            for (int i = start; i < len && i - start <= MAX_CODE_SCAN; i++) {
                char c = message.charAt(i);
                if (c == ':') {
                    colon = i;
                    break;
                }
                if (isSpace(c)) {
                    sawSpace = true;
                    break;
                }
                if (c == '.') {
                    hasDot = true;
                } else if (c == '-' && i + 1 < len && isAsciiDigit(message.charAt(i + 1))) {
                    hasDashDigit = true;
                }
            }
            // A prefix worth dropping is a dotted class name or a vendor code, and
            // must be followed by whitespace so we never cut "10.0.0.1:8081" in half.
            if (colon < 0 || sawSpace || colon == start || !(hasDot || hasDashDigit)) {
                return start;
            }
            int next = colon + 1;
            if (next >= len || !isSpace(message.charAt(next))) {
                return start;
            }
            start = skipSpaces(message, next, len);
            if (start >= len) {
                return start;
            }
        }
        return start;
    }

    /**
     * Decides whether the token {@code [from, to)} is a variable value that should be
     * masked rather than kept.
     *
     * <p>A token is variable when it either
     * <ul>
     *   <li><b>starts with a digit</b> — a measurement or an id, whatever trails it:
     *       {@code 88213}, {@code 30000ms}, {@code 10.200.140.151}, {@code 2026-08-25T10}.
     *       Without this rule {@code "timed out after 30000ms"} and
     *       {@code "timed out after 45000ms"} would be two separate time series; or</li>
     *   <li>is a long letter-led token that mixes in digits — a UUID, a hex digest or a
     *       session id ({@code a3f1e2b4c5d67890}).</li>
     * </ul>
     *
     * <p>Letter-led tokens without digits ({@code PK_SESSION}, {@code DC-DR}) and short
     * ones that merely contain a digit ({@code utf8mb4}) are meaningful names and are kept.
     */
    private static boolean looksVariable(String message, int from, int to) {
        if (from >= to) {
            return false;
        }
        if (isAsciiDigit(message.charAt(from))) {
            return true;
        }
        boolean hasDigit = false;
        for (int i = from + 1; i < to; i++) {
            if (isAsciiDigit(message.charAt(i))) {
                hasDigit = true;
                break;
            }
        }
        return hasDigit && (to - from) >= IDENTIFIER_MIN_LEN;
    }

    /** Appends a placeholder unless the previous emitted character was the same one. */
    private static void appendPlaceholder(StringBuilder out, char placeholder) {
        int n = out.length();
        if (n > 0 && out.charAt(n - 1) == placeholder) {
            return;
        }
        out.append(placeholder);
    }

    private static int skipQuoted(String message, int openIdx, int len, char quote) {
        int i = openIdx + 1;
        while (i < len && message.charAt(i) != quote) {
            i++;
        }
        return i < len ? i + 1 : len;
    }

    private static int skipBracketed(String message, int openIdx, int len, char open) {
        char close = open == '[' ? ']' : '}';
        int i = openIdx + 1;
        while (i < len && message.charAt(i) != close) {
            i++;
        }
        return i < len ? i + 1 : len;
    }

    private static int skipSpaces(String message, int from, int len) {
        int i = from;
        while (i < len && isSpace(message.charAt(i))) {
            i++;
        }
        return i;
    }

    private static void trimTrailing(StringBuilder out) {
        int n = out.length();
        while (n > 0 && (out.charAt(n - 1) == ' ' || out.charAt(n - 1) == ':' || out.charAt(n - 1) == ',')) {
            n--;
        }
        out.setLength(n);
    }

    private static boolean isSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == 0x0B;
    }

    private static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean isTokenStart(char c) {
        return isAsciiLetter(c) || isAsciiDigit(c);
    }

    private static boolean isTokenChar(char c) {
        return isAsciiLetter(c) || isAsciiDigit(c) || c == '.' || c == '_' || c == '-';
    }
}
