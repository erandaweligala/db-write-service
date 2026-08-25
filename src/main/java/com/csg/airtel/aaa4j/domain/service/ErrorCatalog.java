package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.config.ConnectivityMonitoringConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * The answer to "what is actually breaking, and how often?".
 *
 * <p>The pre-existing {@link ExceptionMetricsService} counts exceptions by root-cause
 * <em>class name</em>. In practice that is not enough to act on: a dashboard row reading
 * {@code SQLException — 4,812} tells an operator that something is wrong with the database
 * and nothing more. It does not say whether those are 4,812 duplicate-key rejections
 * (harmless, a bad producer) or 4,812 "no listener" failures (the database is down).
 *
 * <p>This catalog groups by the full identity of a fault instead, and records the four
 * things needed to recognise it at a glance:
 *
 * <table>
 *   <caption>What each entry carries</caption>
 *   <tr><th>Field</th><th>Example</th><th>Source</th></tr>
 *   <tr><td>{@code error}</td><td>{@code SQLException}</td>
 *       <td>root-cause class simple name</td></tr>
 *   <tr><td>{@code code}</td><td>{@code ORA-00001}</td>
 *       <td>vendor / HTTP / SQLState code, {@code none} when absent</td></tr>
 *   <tr><td>{@code reason}</td><td>{@code unique constraint ? violated on id #}</td>
 *       <td>normalised message — the root cause in words</td></tr>
 *   <tr><td>{@code occurrences}</td><td>{@code 4812}</td>
 *       <td>how many times this exact fault happened</td></tr>
 * </table>
 *
 * <p>Each distinct combination is one Prometheus series
 * ({@code application_error_occurrences_total}) and one row in
 * {@code GET /api/monitoring/errors}, sorted by occurrences descending — so the
 * loudest problem is always the top row.
 *
 * <h2>Why this does not blow up Prometheus</h2>
 * Raw exception messages are unique per occurrence and would create an unbounded number
 * of time series. Two defences:
 * <ol>
 *   <li>{@link ErrorSignatures#normalizeReason} masks the variable parts of a message,
 *       so every occurrence of one fault collapses onto one reason template;</li>
 *   <li>a hard ceiling of {@value #MAX_SIGNATURES} distinct signatures. Once reached,
 *       further <em>new</em> signatures are folded into a single {@code (other)} row
 *       rather than registering new meters. Already-known signatures keep counting
 *       normally, so an overflow degrades detail — never accuracy of the total.</li>
 * </ol>
 *
 * <h2>Cost</h2>
 * Nothing here executes on a success path; it runs only after an exception has already
 * been constructed, which is itself far more expensive than anything below.
 * <ul>
 *   <li><b>Repeat occurrence</b> (the overwhelmingly common case): one bounded message
 *       scan, one hash lookup, one {@link LongAdder} increment and one counter increment.
 *       No stack-trace access, no meter registration, no logging, no locking.</li>
 *   <li><b>First sight of a signature</b> (once per distinct fault, ever): additionally
 *       resolves the throwing frame and registers one meter.</li>
 * </ul>
 * All state is bounded and fixed after warm-up, so there is no periodic work and nothing
 * to garbage collect in steady state.
 */
@ApplicationScoped
public class ErrorCatalog {

    private static final String METRIC_OCCURRENCES = "application_error_occurrences";

    private static final String TAG_SERVICE = "service";
    private static final String TAG_ERROR = "error";
    private static final String TAG_CODE = "code";
    private static final String TAG_REASON = "reason";
    private static final String TAG_LAYER = "layer";
    private static final String TAG_SOURCE = "source";

    /** Ceiling on distinct signatures held (and on series exported). */
    static final int MAX_SIGNATURES = 200;

    /** Placeholder used for every field of the overflow row. */
    static final String OVERFLOW = "(other)";

    /** Package prefix used to pick the application frame out of a stack trace. */
    private static final String APP_PACKAGE = "com.csg.airtel";

    /** Frames beyond this depth are not searched when locating the throwing frame. */
    private static final int MAX_FRAME_SCAN = 32;

    private static final String UNKNOWN_ORIGIN = "unknown";

    private final MeterRegistry registry;
    private final String serviceName;

    private final ConcurrentMap<Key, Entry> entries = new ConcurrentHashMap<>();
    private final LongAdder totalOccurrences = new LongAdder();

    @Inject
    public ErrorCatalog(MeterRegistry registry, ConnectivityMonitoringConfig config) {
        this.registry = registry;
        this.serviceName = config.serviceName();
    }

    /**
     * Records one occurrence of {@code root}.
     *
     * <p>Callers do not need to supply the code or the reason — both are derived from the
     * throwable, so no call site has to change to gain the extra detail.
     *
     * @param root      the resolved root cause; {@code null} is ignored
     * @param errorType simple class name of the root cause
     * @param layer     application layer label
     * @param source    originating subsystem label
     */
    public void record(Throwable root, String errorType, String layer, String source) {
        if (root == null || errorType == null) {
            return;
        }
        String message = root.getMessage();
        String code = extractCode(root, message);
        String reason = ErrorSignatures.normalizeReason(message);

        Key key = new Key(errorType, code, reason, safe(layer), safe(source));
        Entry entry = entries.get(key);
        if (entry == null) {
            entry = admit(key, root, message);
        }
        entry.occurrences.increment();
        entry.counter.increment();
        entry.lastSeenEpochMs = System.currentTimeMillis();
        totalOccurrences.increment();
    }

    /**
     * Resolves the entry for a not-yet-seen signature, honouring the cardinality ceiling.
     * Runs at most once per distinct signature.
     */
    private Entry admit(Key key, Throwable root, String message) {
        if (entries.size() >= MAX_SIGNATURES) {
            Key overflowKey = new Key(OVERFLOW, OVERFLOW, OVERFLOW, OVERFLOW, OVERFLOW);
            Entry existing = entries.get(overflowKey);
            if (existing != null) {
                return existing;
            }
            return entries.computeIfAbsent(overflowKey,
                    k -> new Entry(k, newCounter(k),
                            "signatures beyond the " + MAX_SIGNATURES + "-entry ceiling, aggregated",
                            UNKNOWN_ORIGIN));
        }
        return entries.computeIfAbsent(key,
                k -> new Entry(k, newCounter(k), ErrorSignatures.sampleOf(message), originOf(root)));
    }

    private Counter newCounter(Key key) {
        return Counter.builder(METRIC_OCCURRENCES)
                .description("Occurrences of a distinct application error, identified by "
                        + "exception type, error code and normalised reason")
                .tags(Tags.of(
                        TAG_SERVICE, serviceName,
                        TAG_ERROR, key.error(),
                        TAG_CODE, key.code(),
                        TAG_REASON, key.reason(),
                        TAG_LAYER, key.layer(),
                        TAG_SOURCE, key.source()))
                .register(registry);
    }

    /**
     * Pulls a machine-readable code off the throwable, preferring structured accessors
     * over text. Returns {@link ErrorSignatures#NO_CODE} when the throwable exposes none.
     *
     * <p>Ordered most-specific first:
     * <ol>
     *   <li>{@link SQLException} — the vendor code embedded in the message
     *       ({@code ORA-00001}) if present, else the SQLState, else the vendor int;</li>
     *   <li>{@link WebApplicationException} — the HTTP status;</li>
     *   <li>anything else — a {@code LETTERS-DIGITS} code found in the message, which
     *       covers the reactive drivers that surface Oracle errors as plain text.</li>
     * </ol>
     */
    private static String extractCode(Throwable root, String message) {
        if (root instanceof SQLException sql) {
            String fromMessage = ErrorSignatures.codeFromMessage(message);
            if (fromMessage != null) {
                return fromMessage;
            }
            String state = sql.getSQLState();
            if (state != null && !state.isBlank()) {
                return "SQLSTATE-" + state;
            }
            int vendor = sql.getErrorCode();
            if (vendor != 0) {
                return "SQL-" + vendor;
            }
            return ErrorSignatures.NO_CODE;
        }
        if (root instanceof WebApplicationException web) {
            return "HTTP-" + web.getResponse().getStatus();
        }
        String fromMessage = ErrorSignatures.codeFromMessage(message);
        return fromMessage != null ? fromMessage : ErrorSignatures.NO_CODE;
    }

    /**
     * Returns {@code Class.method:line} for the first application frame in the stack, so the
     * catalog says <em>where</em> the fault came from as well as what it was.
     *
     * <p>Touching the stack trace costs O(depth), which is why it happens exactly once per
     * distinct signature rather than once per occurrence.
     */
    private static String originOf(Throwable root) {
        StackTraceElement[] frames;
        try {
            frames = root.getStackTrace();
        } catch (Exception e) {
            return UNKNOWN_ORIGIN;
        }
        if (frames == null || frames.length == 0) {
            return UNKNOWN_ORIGIN;
        }
        int limit = Math.min(frames.length, MAX_FRAME_SCAN);
        for (int i = 0; i < limit; i++) {
            if (frames[i].getClassName().startsWith(APP_PACKAGE)) {
                return format(frames[i]);
            }
        }
        return format(frames[0]);
    }

    private static String format(StackTraceElement frame) {
        String cls = frame.getClassName();
        int dot = cls.lastIndexOf('.');
        return (dot >= 0 ? cls.substring(dot + 1) : cls) + '.' + frame.getMethodName() + ':' + frame.getLineNumber();
    }

    private static String safe(String value) {
        return (value == null || value.isBlank()) ? "unknown" : value;
    }

    /**
     * Immutable snapshot of every known error, ordered by occurrences descending — the
     * loudest problem first. Computed on demand; only the REST endpoint calls this.
     */
    public List<ErrorSummary> snapshot() {
        List<ErrorSummary> out = new ArrayList<>(entries.size());
        for (Entry e : entries.values()) {
            out.add(new ErrorSummary(
                    e.key.error(),
                    e.key.code(),
                    e.key.reason(),
                    e.occurrences.sum(),
                    e.key.layer(),
                    e.key.source(),
                    e.sampleMessage,
                    e.origin,
                    e.firstSeenEpochMs,
                    e.lastSeenEpochMs));
        }
        out.sort(Comparator.comparingLong(ErrorSummary::occurrences).reversed());
        return Collections.unmodifiableList(out);
    }

    /** Total occurrences across every signature, including the overflow row. */
    public long totalOccurrences() {
        return totalOccurrences.sum();
    }

    /** Number of distinct signatures currently held. */
    public int distinctSignatures() {
        return entries.size();
    }

    /** {@code true} once the cardinality ceiling has been reached and detail is being folded. */
    public boolean atCapacity() {
        return entries.size() >= MAX_SIGNATURES;
    }

    /** The identity of a distinct fault. */
    private record Key(String error, String code, String reason, String layer, String source) {
    }

    /** Mutable per-signature state. Only {@code lastSeenEpochMs} changes after admission. */
    private static final class Entry {
        private final Key key;
        private final Counter counter;
        private final String sampleMessage;
        private final String origin;
        private final long firstSeenEpochMs;
        private final LongAdder occurrences = new LongAdder();
        private volatile long lastSeenEpochMs;

        private Entry(Key key, Counter counter, String sampleMessage, String origin) {
            this.key = key;
            this.counter = counter;
            this.sampleMessage = sampleMessage;
            this.origin = origin;
            this.firstSeenEpochMs = System.currentTimeMillis();
            this.lastSeenEpochMs = this.firstSeenEpochMs;
        }
    }

    /**
     * One row of the catalog as served to operators.
     *
     * @param error         root-cause exception class, e.g. {@code SQLException}
     * @param code          vendor/HTTP code, or {@code none}
     * @param reason        normalised message explaining the fault
     * @param occurrences   how many times this fault has happened since startup
     * @param layer         application layer that caught it
     * @param source        originating subsystem
     * @param sampleMessage one real, unredacted message for this signature
     * @param origin        {@code Class.method:line} that threw it
     * @param firstSeen     epoch millis of the first occurrence
     * @param lastSeen      epoch millis of the most recent occurrence
     */
    public record ErrorSummary(String error,
                               String code,
                               String reason,
                               long occurrences,
                               String layer,
                               String source,
                               String sampleMessage,
                               String origin,
                               long firstSeen,
                               long lastSeen) {
    }
}
