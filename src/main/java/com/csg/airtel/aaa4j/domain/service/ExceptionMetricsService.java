package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.quarkus.scheduler.Scheduled;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Aggregates application exceptions by their root-cause type and exposes them
 * to Prometheus via Micrometer.
 *
 * <p>Hot-path is allocation-free after warm-up. Periodic tasks refresh percentage
 * gauges, evict the retry-dedup cache, and reset daily counters at midnight.
 */
@ApplicationScoped
public class ExceptionMetricsService {

    private static final Logger log = Logger.getLogger(ExceptionMetricsService.class);
    private static final String M_INIT = "init";
    private static final String M_RECORD = "recordException";
    private static final String M_REFRESH = "refreshPercentages";
    private static final String M_RESET = "resetDailyCounters";
    private static final String M_EVICT = "evictDedupCache";

    private static final String METRIC_PER_LAYER = "application_exception_count";
    private static final String METRIC_ROOT_TOTAL = "application_exception_root_count";
    private static final String METRIC_PERCENTAGE = "application_exception_percentage";
    private static final String METRIC_TOTAL = "application_exception_total";

    private static final String TAG_EXCEPTION_TYPE = "exception_type";
    private static final String TAG_LAYER = "layer";
    private static final String TAG_SOURCE = "source";

    /** Hard cap on cause-chain walking; chains deeper than this are vanishingly rare. */
    private static final int MAX_CAUSE_DEPTH = 16;

    /** Retry dedup window: identical observations within this window are counted once. */
    static final long DEDUP_TTL_MILLIS = 5_000L;

    /** Hard cap on dedup cache size so a misbehaving caller can't grow it without bound. */
    private static final int DEDUP_MAX_ENTRIES = 8_192;

    /** Application layers used as the {@code layer} tag value. */
    public enum Layer {
        RESOURCE("resource"),
        SERVICE("service"),
        REPOSITORY("repository"),
        CONSUMER("consumer"),
        INFRASTRUCTURE("infrastructure");

        static final Layer[] VALUES = values();

        private final String label;

        Layer(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    /**
     * Originating subsystem of the exception. Lets dashboards tell an Oracle
     * stall apart from a MySQL or Kafka stall when the root cause class
     * (e.g. {@code ConnectionException}, {@code TimeoutException}) is generic.
     */
    public enum Source {
        KAFKA("kafka"),
        ORACLE("oracle"),
        MYSQL("mysql"),
        REDIS("redis"),
        INTERNAL("internal"),
        UNKNOWN("unknown");

        static final Source[] VALUES = values();

        private final String label;

        Source(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private final MeterRegistry registry;

    /** exceptionType -> Counter[layer.ordinal()][source.ordinal()] — avoids per-call key concatenation. */
    private final ConcurrentMap<String, Counter[][]> perLayerCounters = new ConcurrentHashMap<>();
    /** exceptionType -> root Counter (aggregated across layers and sources). */
    private final ConcurrentMap<String, Counter> rootCounters = new ConcurrentHashMap<>();
    /** exceptionType -> daily AtomicLong (resets at 00:00). */
    private final ConcurrentMap<String, AtomicLong> dailyRootCounts = new ConcurrentHashMap<>();

    private final DoubleAdder totalRootCount = new DoubleAdder();

    /** Fingerprint -> expiry epoch millis. Suppresses retry-attempt duplicates within a short window. */
    private final ConcurrentMap<String, Long> dedupCache = new ConcurrentHashMap<>();

    private MultiGauge percentageGauge;

    @Inject
    public ExceptionMetricsService(MeterRegistry registry) {
        this.registry = registry;
    }

    @PostConstruct
    void init() {
        LoggingUtil.logInfo(log, M_INIT, "Initializing ExceptionMetricsService");

        this.percentageGauge = MultiGauge.builder(METRIC_PERCENTAGE)
                .description("Percentage of total application exceptions attributable to each root exception type")
                .baseUnit("percent")
                .register(registry);

        Gauge.builder(METRIC_TOTAL, totalRootCount, DoubleAdder::sum)
                .description("Sum of all root exception occurrences observed since startup")
                .register(registry);
    }

    /**
     * Backwards-compatible entry point. Callers that have not yet been updated to
     * provide a {@link Source} are attributed to {@link Source#UNKNOWN}.
     */
    public void recordException(Throwable throwable, Layer layer) {
        recordException(throwable, layer, Source.UNKNOWN);
    }

    /**
     * Records an exception observation. Hot-path: zero allocation after warm-up
     * for non-deduped calls.
     *
     * @param throwable the caught throwable; {@code null} is ignored
     * @param layer     the application layer where the exception was caught; {@code null} is ignored
     * @param source    the originating subsystem; {@code null} is treated as {@link Source#UNKNOWN}
     */
    public void recordException(Throwable throwable, Layer layer, Source source) {
        if (throwable == null || layer == null) {
            return;
        }
        Source src = source == null ? Source.UNKNOWN : source;
        try {
            Throwable root = resolveRootCause(throwable);
            String type = simpleName(root);

            if (!markRecorded(root)) {
                return;
            }

            if (isDuplicateRetry(type, layer, src)) {
                return;
            }

            incrementCounters(type, layer, src);
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_RECORD, "Failed to record exception metric: %s", e.getMessage());
        }
    }

    private void incrementCounters(String type, Layer layer, Source source) {
        Counter[][] layerMatrix = perLayerCounters.get(type);
        if (layerMatrix == null) {
            layerMatrix = perLayerCounters.computeIfAbsent(type,
                    k -> new Counter[Layer.VALUES.length][Source.VALUES.length]);
        }
        int layerIdx = layer.ordinal();
        int sourceIdx = source.ordinal();
        Counter perLayer = layerMatrix[layerIdx][sourceIdx];
        if (perLayer == null) {
            perLayer = Counter.builder(METRIC_PER_LAYER)
                    .description("Application exceptions broken down by root exception type, layer, and source")
                    .tags(Tags.of(
                            TAG_EXCEPTION_TYPE, type,
                            TAG_LAYER, layer.label(),
                            TAG_SOURCE, source.label()))
                    .register(registry);
            layerMatrix[layerIdx][sourceIdx] = perLayer;
        }
        perLayer.increment();

        Counter root = rootCounters.get(type);
        if (root == null) {
            root = rootCounters.computeIfAbsent(type, k -> Counter.builder(METRIC_ROOT_TOTAL)
                    .description("Application exceptions aggregated across all layers/sources by root exception type")
                    .tag(TAG_EXCEPTION_TYPE, k)
                    .register(registry));
        }
        root.increment();

        AtomicLong daily = dailyRootCounts.get(type);
        if (daily == null) {
            daily = dailyRootCounts.computeIfAbsent(type, k -> new AtomicLong());
        }
        daily.incrementAndGet();

        totalRootCount.add(1.0);
    }

    /**
     * Walks the cause chain to the deepest non-null, non-self-referential cause.
     * Depth-bounded to {@value #MAX_CAUSE_DEPTH} hops.
     */
    private static Throwable resolveRootCause(Throwable t) {
        Throwable cur = t;
        for (int i = 0; i < MAX_CAUSE_DEPTH; i++) {
            Throwable cause = cur.getCause();
            if (cause == null || cause == cur) {
                break;
            }
            cur = cause;
        }
        return cur;
    }

    private static String simpleName(Throwable t) {
        String n = t.getClass().getSimpleName();
        return n.isEmpty() ? t.getClass().getName() : n;
    }

    /** Retained for tests / external callers — resolves and returns the simple-name. */
    static String resolveRootCauseType(Throwable t) {
        return simpleName(resolveRootCause(t));
    }

    /**
     * Marks {@code root} as recorded by attaching a {@link RecordedMarker} sentinel
     * to its suppressed list. Returns {@code true} if the root was not previously
     * marked (caller should record), {@code false} if already marked.
     */
    private static boolean markRecorded(Throwable root) {
        Throwable[] suppressed = root.getSuppressed();
        for (Throwable s : suppressed) {
            if (s instanceof RecordedMarker) {
                return false;
            }
        }
        try {
            root.addSuppressed(new RecordedMarker());
        } catch (Throwable ignore) {
            // Some throwables disable suppression; fall through and record anyway.
        }
        return true;
    }

    /**
     * Returns {@code true} if a record for the same {@code (type, layer, source)}
     * within the current Vert.x context was observed within {@link #DEDUP_TTL_MILLIS}.
     * Otherwise stamps a fresh entry and returns {@code false}.
     */
    private boolean isDuplicateRetry(String type, Layer layer, Source source) {
        String ctxKey = contextKey();
        String key = type + '|' + layer.ordinal() + '|' + source.ordinal() + '|' + ctxKey;
        long now = System.currentTimeMillis();
        long expiry = now + DEDUP_TTL_MILLIS;

        Long existing = dedupCache.putIfAbsent(key, expiry);
        if (existing == null) {
            if (dedupCache.size() > DEDUP_MAX_ENTRIES) {
                evictExpired(now);
            }
            return false;
        }
        if (existing <= now) {
            dedupCache.put(key, expiry);
            return false;
        }
        return true;
    }

    private static String contextKey() {
        try {
            Context ctx = Vertx.currentContext();
            if (ctx != null) {
                return "v" + System.identityHashCode(ctx);
            }
        } catch (Throwable ignore) {
            // Fall through to thread-name fallback.
        }
        return "t" + Thread.currentThread().getName();
    }

    /** Periodic sweep of expired dedup entries. */
    @Scheduled(every = "10s")
    void evictDedupCache() {
        try {
            evictExpired(System.currentTimeMillis());
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_EVICT, "Failed to evict dedup cache entries: %s", e.getMessage());
        }
    }

    private void evictExpired(long now) {
        Iterator<Map.Entry<String, Long>> it = dedupCache.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue() <= now) {
                it.remove();
            }
        }
    }

    /**
     * Recomputes the percentage gauge rows by reading directly from the root counters.
     * Runs on a 30s schedule — the hot path never touches this work.
     */
    @Scheduled(every = "30s")
    void refreshPercentages() {
        try {
            MultiGauge g = percentageGauge;
            if (g == null) {
                return;
            }
            double total = totalRootCount.sum();
            if (total <= 0.0) {
                return;
            }
            List<MultiGauge.Row<?>> rows = new ArrayList<>(rootCounters.size());
            for (Map.Entry<String, Counter> entry : rootCounters.entrySet()) {
                double pct = (entry.getValue().count() / total) * 100.0;
                rows.add(MultiGauge.Row.of(Tags.of(Tag.of(TAG_EXCEPTION_TYPE, entry.getKey())), pct));
            }
            g.register(rows, true);
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_REFRESH, "Failed to refresh percentage gauges: %s", e.getMessage());
        }
    }

    /**
     * Resets the 24-hour daily exception counters at midnight. Lifetime counters are untouched.
     */
    @Scheduled(cron = "0 0 0 * * ?")
    void resetDailyCounters() {
        LoggingUtil.logInfo(log, M_RESET, "Resetting daily exception counters");
        for (AtomicLong v : dailyRootCounts.values()) {
            v.set(0);
        }
    }

    /**
     * Returns an immutable snapshot of {@code exceptionType -> {count, percentage, daily}}
     * sorted by descending count. Computed on demand — only the REST endpoint calls this.
     */
    public Map<String, ExceptionStats> snapshot() {
        double total = totalRootCount.sum();
        List<Map.Entry<String, Counter>> entries = new ArrayList<>(rootCounters.entrySet());
        entries.sort((a, b) -> Double.compare(b.getValue().count(), a.getValue().count()));

        Map<String, ExceptionStats> out = LinkedHashMap.newLinkedHashMap(entries.size());
        for (Map.Entry<String, Counter> e : entries) {
            double count = e.getValue().count();
            double pct = total > 0.0 ? (count / total) * 100.0 : 0.0;
            AtomicLong d = dailyRootCounts.get(e.getKey());
            long daily = d == null ? 0L : d.get();
            out.put(e.getKey(), new ExceptionStats((long) count, pct, daily));
        }
        return Collections.unmodifiableMap(out);
    }

    public long getTotalRootCount() {
        return (long) totalRootCount.sum();
    }

    /** Immutable view of one exception type's aggregated stats. */
    public record ExceptionStats(long count, double percentage, long dailyCount) {}

    /**
     * Sentinel attached as a suppressed exception to mark a root cause as already
     * counted. Stack trace and suppression are disabled to keep it allocation-cheap.
     */
    private static final class RecordedMarker extends Throwable {
        private static final long serialVersionUID = 1L;
        RecordedMarker() {
            super("EMS_RECORDED", null, false, false);
        }
    }
}
