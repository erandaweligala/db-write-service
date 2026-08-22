package com.csg.airtel.aaa4j.infrastructure;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Counts messages that reach a terminal (non-retryable) state at a Kafka consumer,
 * broken down by {@code channel} and {@code reason}, and exposes them to Prometheus
 * via Micrometer.
 */
@ApplicationScoped
public class DlqMetrics {

    private static final Logger log = Logger.getLogger(DlqMetrics.class);
    private static final String M_RECORD = "record";

    // Meter base names. Micrometer's Prometheus registry appends the "_total"
    // suffix for counters, so these export as kafka_dlq_events_total and
    // kafka_dropped_events_total on /q/metrics.
    static final String METRIC_DLQ = "kafka.dlq.events";
    static final String METRIC_DROPPED = "kafka.dropped.events";
    // Exported as kafka_dlq_reprocess_total, tagged by topic and outcome.
    static final String METRIC_REPROCESS = "kafka.dlq.reprocess";

    private static final String TAG_CHANNEL = "channel";
    private static final String TAG_REASON = "reason";
    private static final String TAG_TOPIC = "topic";
    private static final String TAG_OUTCOME = "outcome";

    private static final String UNKNOWN_CHANNEL = "unknown";
    private static final String UNKNOWN_TOPIC = "unknown";

    /**
     * Why a message reached a terminal state. Kept coarse on purpose so the
     * cardinality of the {@code reason} tag stays bounded.
     */
    public enum Reason {
        DESERIALIZATION("deserialization"),
        DB_PERMANENT("db_permanent"),
        DB_TRANSIENT_EXHAUSTED("db_transient_exhausted"),
        MISSING_HEADERS("missing_headers"),
        UNKNOWN("unknown");

        private final String label;

        Reason(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    /**
     * Disposition of a record handled by the DLQ reprocessor. Mirrors
     * {@link com.csg.airtel.aaa4j.infrastructure.dlq.DlqReprocessor.Outcome}.
     */
    public enum ReprocessOutcome {
        /** Replayed to the DB successfully; removed from the DLT. */
        SUCCEEDED("succeeded"),
        /** Replay failed but attempts remain; re-queued to the DLT for a later run. */
        REQUEUED("requeued"),
        /** Replay failed with attempts exhausted (or unparseable); moved to the parked topic. */
        PARKED("parked");

        private final String label;

        ReprocessOutcome(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private final MeterRegistry registry;

    private final ConcurrentMap<String, Counter> dlqCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> droppedCounters = new ConcurrentHashMap<>();
    /** "topic|outcome" -> Counter, for records handled by the reprocessor. */
    private final ConcurrentMap<String, Counter> reprocessCounters = new ConcurrentHashMap<>();

    @Inject
    public DlqMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // =========================================================================
    // Dead-letter disposition (failure-strategy: dead-letter-queue)
    // =========================================================================

    /**
     * Records that a message on {@code channel} was routed to its dead-letter topic
     * for the given {@code reason}.
     */
    public void recordDlqEvent(String channel, Reason reason) {
        increment(dlqCounters, METRIC_DLQ, channel, reason);
    }

    /**
     * Convenience overload that classifies {@code throwable} into a {@link Reason}
     * ({@link Reason#DB_TRANSIENT_EXHAUSTED} vs {@link Reason#DB_PERMANENT}).
     */
    public void recordDlqEvent(String channel, Throwable throwable) {
        recordDlqEvent(channel, classify(throwable));
    }

    // =========================================================================
    // Dropped disposition (failure-strategy: ignore)
    // =========================================================================

    /**
     * Records that a message on {@code channel} was acknowledged and discarded
     * (payload lost) for the given {@code reason}.
     */
    public void recordDroppedEvent(String channel, Reason reason) {
        increment(droppedCounters, METRIC_DROPPED, channel, reason);
    }

    /**
     * Convenience overload that classifies {@code throwable} into a {@link Reason}.
     */
    public void recordDroppedEvent(String channel, Throwable throwable) {
        recordDroppedEvent(channel, classify(throwable));
    }

    // =========================================================================
    // Reprocess disposition (DLQ replay outcomes)
    // =========================================================================

    /**
     * Records the outcome of replaying one dead-letter record on {@code topic}
     * (succeeded / requeued / parked). Exported as {@code kafka_dlq_reprocess_total},
     * tagged by {@code topic} and {@code outcome}.
     */
    public void recordReprocess(String topic, ReprocessOutcome outcome) {
        try {
            String t = (topic == null || topic.isBlank()) ? UNKNOWN_TOPIC : topic;
            ReprocessOutcome o = outcome == null ? ReprocessOutcome.PARKED : outcome;
            String key = t + '|' + o.label();
            Counter counter = reprocessCounters.get(key);
            if (counter == null) {
                counter = reprocessCounters.computeIfAbsent(key, k -> Counter.builder(METRIC_REPROCESS)
                        .description("DLQ records handled by the reprocessor, by topic and outcome")
                        .tags(Tags.of(TAG_TOPIC, t, TAG_OUTCOME, o.label()))
                        .register(registry));
            }
            counter.increment();
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_RECORD, "Failed to record reprocess metric: %s", e.getMessage());
        }
    }

    // =========================================================================
    // Internals
    // =========================================================================

    private void increment(ConcurrentMap<String, Counter> cache, String metric,
                           String channel, Reason reason) {
        try {
            String ch = (channel == null || channel.isBlank()) ? UNKNOWN_CHANNEL : channel;
            Reason r = reason == null ? Reason.UNKNOWN : reason;
            String key = ch + '|' + r.label();
            Counter counter = cache.get(key);
            if (counter == null) {
                counter = cache.computeIfAbsent(key, k -> Counter.builder(metric)
                        .description("Kafka messages that reached a terminal state at a consumer, "
                                + "by channel and reason")
                        .tags(Tags.of(TAG_CHANNEL, ch, TAG_REASON, r.label()))
                        .register(registry));
            }
            counter.increment();
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_RECORD, "Failed to record DLQ metric (%s): %s",
                    metric, e.getMessage());
        }
    }

    /**
     * Classifies a terminal DB failure. Transient failures that exhausted their
     * retries are distinguished from permanent (poison) failures so a spike in
     * {@link Reason#DB_TRANSIENT_EXHAUSTED} can be read as "the DB was down long
     * enough to blow the retry budget" rather than "bad data".
     */
    static Reason classify(Throwable throwable) {
        if (throwable == null) {
            return Reason.UNKNOWN;
        }
        return KafkaFailureClassifier.isTransient(throwable)
                ? Reason.DB_TRANSIENT_EXHAUSTED
                : Reason.DB_PERMANENT;
    }

    /**
     * Immutable snapshot of the current counts, suitable for a JSON endpoint.
     * Shape: {@code {dlqTotal, droppedTotal, dlqByChannel:{...}, droppedByChannel:{...}}}.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("dlqTotal", (long) sum(dlqCounters));
        out.put("droppedTotal", (long) sum(droppedCounters));
        out.put("reprocessTotal", (long) sum(reprocessCounters));
        out.put("dlq", breakdown(dlqCounters));
        out.put("dropped", breakdown(droppedCounters));
        out.put("reprocess", breakdown(reprocessCounters));
        return Collections.unmodifiableMap(out);
    }

    public long getReprocessTotal() {
        return (long) sum(reprocessCounters);
    }

    public long getDlqTotal() {
        return (long) sum(dlqCounters);
    }

    public long getDroppedTotal() {
        return (long) sum(droppedCounters);
    }

    private static double sum(ConcurrentMap<String, Counter> cache) {
        double total = 0.0;
        for (Counter c : cache.values()) {
            total += c.count();
        }
        return total;
    }

    /** channel -> reason -> count, sorted by descending count within each channel. */
    private static Map<String, Object> breakdown(ConcurrentMap<String, Counter> cache) {
        Map<String, List<Map.Entry<String, Long>>> byChannel = new LinkedHashMap<>();
        for (Map.Entry<String, Counter> e : cache.entrySet()) {
            int sep = e.getKey().indexOf('|');
            String channel = sep < 0 ? e.getKey() : e.getKey().substring(0, sep);
            String reason = sep < 0 ? UNKNOWN_CHANNEL : e.getKey().substring(sep + 1);
            byChannel.computeIfAbsent(channel, k -> new ArrayList<>())
                    .add(Map.entry(reason, (long) e.getValue().count()));
        }

        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Map.Entry<String, Long>>> e : byChannel.entrySet()) {
            List<Map.Entry<String, Long>> reasons = e.getValue();
            reasons.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));
            Map<String, Long> reasonMap = new LinkedHashMap<>();
            for (Map.Entry<String, Long> r : reasons) {
                reasonMap.put(r.getKey(), r.getValue());
            }
            out.put(e.getKey(), reasonMap);
        }
        return out;
    }
}
