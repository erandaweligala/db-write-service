package com.csg.airtel.aaa4j.infrastructure.dlq;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.application.common.TraceIdGenerator;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.infrastructure.DlqMetrics;
import com.csg.airtel.aaa4j.infrastructure.ResilientDbWriteExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Replays messages sitting in the Kafka dead-letter topics back through the normal DB-write
 * path, so records that were dead-lettered during a DB outage (or a since-fixed bug) can be
 * recovered without data loss.
 *
 * <h2>Why on-demand rather than a standing consumer</h2>
 * A permanently-attached {@code @Incoming} channel on a DLT would tight-loop on genuine poison
 * messages. Instead this is a <b>bounded, explicitly-triggered drain</b>: an operator (via the
 * {@code /api/dlq} REST surface) or an optional scheduler kicks a run that reads at most
 * {@code max-messages} records that existed when the run started, replays each, and stops.
 *
 * <h2>No-loss semantics</h2>
 * <ul>
 *   <li><b>Success</b> — the DB write is replayed through {@link ResilientDbWriteExecutor}
 *       (same circuit-breaker + retry as live traffic); the consumer offset is committed.</li>
 *   <li><b>Still failing, attempts remain</b> — the original record (payload + headers) is
 *       re-published to the same DLT with an incremented {@value #HEADER_ATTEMPTS} header, to be
 *       retried on a later run.</li>
 *   <li><b>Attempts exhausted / unparseable payload</b> — the record is moved to the parked
 *       topic ({@code <dlt><parked-suffix>}) for manual triage.</li>
 * </ul>
 * Because every consumed record is either replayed, re-queued, or parked <i>before</i> the offset
 * is committed — and commits are capped at the run's starting end-offsets — nothing is dropped and
 * re-queued records are never skipped or re-processed within the same run. Re-publishes are
 * verified after the flush: if any re-queue/park send failed (e.g. the parked topic is missing
 * and auto-create is off), the run aborts <i>before</i> the offset commit so the affected records
 * are re-read on the next run instead of vanishing.
 */
@ApplicationScoped
public class DlqReprocessor {

    private static final Logger log = Logger.getLogger(DlqReprocessor.class);

    /** Number of prior reprocess attempts carried on a re-queued record (absent == 0). */
    static final String HEADER_ATTEMPTS = "x-dlq-reprocess-attempts";
    /** DLT the record was first read from (set when parking, for triage). */
    static final String HEADER_ORIGINAL_TOPIC = "x-dlq-original-topic";
    /** Wall-clock time of the last reprocess attempt. */
    static final String HEADER_LAST_ATTEMPT_AT = "x-dlq-reprocess-at";
    /** Truncated message of the last replay failure. */
    static final String HEADER_LAST_ERROR = "x-dlq-reprocess-error";

    private static final int MAX_ERROR_HEADER_LEN = 500;

    private final KafkaClientFactory clientFactory;
    private final ResilientDbWriteExecutor executor;
    private final ObjectMapper objectMapper;
    private final DlqMetrics dlqMetrics;

    /** Guards against overlapping runs (manual + scheduled) sharing a consumer group. */
    private final ReentrantLock runLock = new ReentrantLock();

    /**
     * First re-publish (re-queue/park) failure of the current batch, captured by the producer
     * callback. Checked after each flush and, if set, aborts the run before the offset commit —
     * a record whose park/re-queue never landed must not be removed from the source DLT.
     * Runs are serialized by {@link #runLock}, so a single slot is sufficient.
     */
    private final AtomicReference<Exception> publishFailure = new AtomicReference<>();

    @ConfigProperty(name = "dlq.reprocess.enabled", defaultValue = "true")
    boolean enabled;

    @ConfigProperty(name = "dlq.reprocess.topics", defaultValue = "")
    List<String> topics;

    @ConfigProperty(name = "dlq.reprocess.group-id", defaultValue = "db-write-dlq-reprocessor")
    String groupId;

    @ConfigProperty(name = "dlq.reprocess.max-messages", defaultValue = "500")
    int defaultMaxMessages;

    @ConfigProperty(name = "dlq.reprocess.poll-timeout-ms", defaultValue = "2000")
    long pollTimeoutMs;

    @ConfigProperty(name = "dlq.reprocess.time-budget-ms", defaultValue = "60000")
    long timeBudgetMs;

    @ConfigProperty(name = "dlq.reprocess.max-attempts", defaultValue = "3")
    int maxAttempts;

    @ConfigProperty(name = "dlq.reprocess.parked-topic-suffix", defaultValue = ".PARKED")
    String parkedSuffix;

    @ConfigProperty(name = "dlq.reprocess.replay-timeout-ms", defaultValue = "30000")
    long replayTimeoutMs;

    @Inject
    public DlqReprocessor(KafkaClientFactory clientFactory,
                          ResilientDbWriteExecutor executor,
                          ObjectMapper objectMapper,
                          DlqMetrics dlqMetrics) {
        this.clientFactory = clientFactory;
        this.executor = executor;
        this.objectMapper = objectMapper;
        this.dlqMetrics = dlqMetrics;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** The configured, reprocess-able DLT topics (the allow-list the REST surface validates against). */
    public List<String> configuredTopics() {
        List<String> out = new ArrayList<>();
        if (topics != null) {
            for (String t : topics) {
                if (t != null && !t.isBlank()) {
                    out.add(t.trim());
                }
            }
        }
        return out;
    }

    /**
     * Reprocess a single DLT topic. {@code maxMessages <= 0} falls back to the configured default.
     * Rejects topics outside the configured allow-list so this cannot be used to drain arbitrary topics.
     */
    public ReprocessSummary reprocessTopic(String topic, int maxMessages) {
        if (!enabled) {
            return ReprocessSummary.status(topic, ReprocessSummary.Status.DISABLED);
        }
        if (topic == null || !configuredTopics().contains(topic)) {
            return ReprocessSummary.status(topic, ReprocessSummary.Status.UNKNOWN_TOPIC);
        }
        if (!runLock.tryLock()) {
            return ReprocessSummary.status(topic, ReprocessSummary.Status.BUSY);
        }
        try {
            return drainTopic(topic, effectiveMax(maxMessages));
        } finally {
            runLock.unlock();
        }
    }

    /**
     * Reprocess every configured DLT topic in sequence under a single lock, returning a
     * per-topic summary keyed by topic name.
     */
    public Map<String, ReprocessSummary> reprocessAll(int maxMessages) {
        Map<String, ReprocessSummary> results = new HashMap<>();
        List<String> configured = configuredTopics();
        if (!enabled) {
            for (String t : configured) {
                results.put(t, ReprocessSummary.status(t, ReprocessSummary.Status.DISABLED));
            }
            return results;
        }
        if (!runLock.tryLock()) {
            for (String t : configured) {
                results.put(t, ReprocessSummary.status(t, ReprocessSummary.Status.BUSY));
            }
            return results;
        }
        try {
            int max = effectiveMax(maxMessages);
            for (String t : configured) {
                results.put(t, drainTopic(t, max));
            }
            return results;
        } finally {
            runLock.unlock();
        }
    }

    private int effectiveMax(int requested) {
        return requested > 0 ? requested : defaultMaxMessages;
    }

    // =========================================================================
    // Drain loop
    // =========================================================================

    private ReprocessSummary drainTopic(String topic, int maxMessages) {
        long start = System.currentTimeMillis();
        long succeeded = 0;
        long requeued = 0;
        long parked = 0;
        long total = 0;
        publishFailure.set(null);

        try (Consumer<String, byte[]> consumer = clientFactory.createConsumer(groupId);
             Producer<String, byte[]> producer = clientFactory.createProducer()) {

            // Provision the parked topic up front (best-effort) so park() cannot fail with
            // UNKNOWN_TOPIC_OR_PARTITION on clusters where topic auto-creation is disabled.
            clientFactory.ensureParkedTopic(topic, topic + parkedSuffix);

            List<TopicPartition> partitions = assign(consumer, topic);
            if (partitions.isEmpty()) {
                LoggingUtil.logWarn(log, "drainTopic",
                        "DLQ reprocess found no partitions for topic=%s — nothing to do", topic);
                return ReprocessSummary.status(topic, ReprocessSummary.Status.NO_PARTITIONS);
            }

            // Snapshot the log-end offsets so records we re-queue during THIS run (appended past
            // these offsets) are not consumed again until a future run.
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            seekToCommittedOrBeginning(consumer, partitions);

            LoggingUtil.logInfo(log, "drainTopic",
                    "DLQ reprocess starting topic=%s partitions=%d maxMessages=%d",
                    topic, partitions.size(), maxMessages);

            long deadline = start + timeBudgetMs;

            while (total < maxMessages && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                if (records.isEmpty()) {
                    break; // drained: nothing new within the poll timeout
                }

                boolean produced = false;
                for (ConsumerRecord<String, byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    Long end = endOffsets.get(tp);
                    if (end != null && record.offset() >= end) {
                        continue; // appended after the run started (re-queued) — leave for next run
                    }
                    if (total >= maxMessages) {
                        break;
                    }
                    Outcome outcome = handleRecord(record, topic, producer);
                    total++;
                    switch (outcome) {
                        case SUCCEEDED -> succeeded++;
                        case REQUEUED -> {
                            requeued++;
                            produced = true;
                        }
                        case PARKED -> {
                            parked++;
                            produced = true;
                        }
                    }
                }

                // Durability barrier: make re-queued / parked records durable before the commit
                // that removes them from the source DLT. flush() alone does not surface send
                // failures, so explicitly check the callback-captured outcome — committing after
                // a failed park would silently drop the record.
                if (produced) {
                    producer.flush();
                    failIfPublishFailed(topic);
                }
                commitCapped(consumer, partitions, endOffsets);

                if (allDrained(consumer, partitions, endOffsets)) {
                    break;
                }
            }

            LoggingUtil.logInfo(log, "drainTopic",
                    "DLQ reprocess finished topic=%s total=%d succeeded=%d requeued=%d parked=%d durationMs=%d",
                    topic, total, succeeded, requeued, parked, System.currentTimeMillis() - start);

            return new ReprocessSummary(topic, ReprocessSummary.Status.COMPLETED,
                    total, succeeded, requeued, parked, System.currentTimeMillis() - start);

        } catch (Exception e) {
            LoggingUtil.logError(log, "drainTopic", e,
                    "DLQ reprocess aborted topic=%s after total=%d (succeeded=%d requeued=%d parked=%d)",
                    topic, total, succeeded, requeued, parked);
            return new ReprocessSummary(topic, ReprocessSummary.Status.ERROR,
                    total, succeeded, requeued, parked, System.currentTimeMillis() - start);
        }
    }

    private List<TopicPartition> assign(Consumer<String, byte[]> consumer, String topic) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList<>();
        if (infos != null) {
            for (PartitionInfo info : infos) {
                partitions.add(new TopicPartition(topic, info.partition()));
            }
        }
        consumer.assign(partitions);
        return partitions;
    }

    private void seekToCommittedOrBeginning(Consumer<String, byte[]> consumer, List<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions));
        List<TopicPartition> fromBeginning = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            OffsetAndMetadata om = committed.get(tp);
            if (om != null) {
                consumer.seek(tp, om.offset());
            } else {
                fromBeginning.add(tp);
            }
        }
        if (!fromBeginning.isEmpty()) {
            consumer.seekToBeginning(fromBeginning);
        }
    }

    /** Commit progress, capped at the run's starting end-offsets so re-queued records survive. */
    /**
     * Aborts the run if any re-queue/park send of the just-flushed batch failed. Called after
     * {@code producer.flush()} (which guarantees all callbacks have fired) and before the offset
     * commit, so the affected records stay on the source DLT and are re-read on the next run.
     * Already-verified earlier batches keep their commits; only the failed batch is replayed.
     */
    void failIfPublishFailed(String topic) {
        Exception failure = publishFailure.getAndSet(null);
        if (failure != null) {
            throw new IllegalStateException(
                    "Re-publish (re-queue/park) failed during DLQ reprocess of " + topic
                            + " — aborting before offset commit so no record is lost. "
                            + "If the cause is UNKNOWN_TOPIC_OR_PARTITION, create the parked topic "
                            + topic + parkedSuffix + " (or grant the reprocessor topic-create permission).",
                    failure);
        }
    }

    private void commitCapped(Consumer<String, byte[]> consumer, List<TopicPartition> partitions,
                              Map<TopicPartition, Long> endOffsets) {
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (TopicPartition tp : partitions) {
            long position = consumer.position(tp);
            Long end = endOffsets.get(tp);
            long capped = end != null ? Math.min(position, end) : position;
            toCommit.put(tp, new OffsetAndMetadata(capped));
        }
        consumer.commitSync(toCommit);
    }

    private boolean allDrained(Consumer<String, byte[]> consumer, List<TopicPartition> partitions,
                               Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition tp : partitions) {
            Long end = endOffsets.get(tp);
            if (end != null && consumer.position(tp) < end) {
                return false;
            }
        }
        return true;
    }

    // =========================================================================
    // Per-record replay
    // =========================================================================

    enum Outcome { SUCCEEDED, REQUEUED, PARKED }

    /**
     * Replays one dead-letter record. Package-private so the decision logic can be unit-tested
     * with a {@code MockProducer} independently of the poll loop.
     */
    Outcome handleRecord(ConsumerRecord<String, byte[]> record, String topic,
                         Producer<String, byte[]> producer) {
        int priorAttempts = readAttempts(record);

        DBWriteRequest request;
        try {
            request = objectMapper.readValue(record.value(), DBWriteRequest.class);
        } catch (Exception e) {
            // Unparseable payload — retrying the parse can never help, so park immediately.
            LoggingUtil.logError(log, "handleRecord", e,
                    "DLQ record on %s is unparseable (offset=%d) — parking", topic, record.offset());
            park(producer, record, topic, priorAttempts, "unparseable: " + e.getMessage());
            dlqMetrics.recordReprocess(topic, DlqMetrics.ReprocessOutcome.PARKED);
            return Outcome.PARKED;
        }

        if (request.getTraceId() == null) {
            request.setTraceId(TraceIdGenerator.generateTraceId());
        }

        try {
            executor.execute(request).await().atMost(Duration.ofMillis(replayTimeoutMs));
            LoggingUtil.logInfo(log, "handleRecord",
                    "DLQ replay succeeded topic=%s user=%s eventType=%s table=%s attempt=%d",
                    topic, request.getUserName(), request.getEventType(), request.getTableName(),
                    priorAttempts + 1);
            dlqMetrics.recordReprocess(topic, DlqMetrics.ReprocessOutcome.SUCCEEDED);
            return Outcome.SUCCEEDED;
        } catch (Exception e) {
            int attempts = priorAttempts + 1;
            if (attempts >= maxAttempts) {
                LoggingUtil.logError(log, "handleRecord", e,
                        "DLQ replay failed topic=%s user=%s eventType=%s attempts=%d — parking",
                        topic, request.getUserName(), request.getEventType(), attempts);
                park(producer, record, topic, priorAttempts, e.getMessage());
                dlqMetrics.recordReprocess(topic, DlqMetrics.ReprocessOutcome.PARKED);
                return Outcome.PARKED;
            }
            LoggingUtil.logWarn(log, "handleRecord",
                    "DLQ replay failed topic=%s user=%s eventType=%s attempts=%d — re-queueing: %s",
                    topic, request.getUserName(), request.getEventType(), attempts, e.getMessage());
            requeue(producer, record, topic, priorAttempts, e.getMessage());
            dlqMetrics.recordReprocess(topic, DlqMetrics.ReprocessOutcome.REQUEUED);
            return Outcome.REQUEUED;
        }
    }

    private void requeue(Producer<String, byte[]> producer, ConsumerRecord<String, byte[]> record,
                         String topic, int priorAttempts, String error) {
        republish(producer, topic, record, priorAttempts + 1, error);
    }

    private void park(Producer<String, byte[]> producer, ConsumerRecord<String, byte[]> record,
                      String topic, int priorAttempts, String error) {
        republish(producer, topic + parkedSuffix, record, priorAttempts + 1, error);
    }

    private void republish(Producer<String, byte[]> producer, String targetTopic,
                           ConsumerRecord<String, byte[]> record, int newAttempts, String error) {
        ProducerRecord<String, byte[]> out = new ProducerRecord<>(targetTopic, record.key(), record.value());
        // Preserve the original headers (dead-letter metadata etc.), replacing only the ones we own.
        for (Header h : record.headers()) {
            if (isReprocessHeader(h.key())) {
                continue;
            }
            out.headers().add(h);
        }
        out.headers().add(HEADER_ATTEMPTS, String.valueOf(newAttempts).getBytes(StandardCharsets.UTF_8));
        out.headers().add(HEADER_ORIGINAL_TOPIC, record.topic().getBytes(StandardCharsets.UTF_8));
        out.headers().add(HEADER_LAST_ATTEMPT_AT, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        if (error != null) {
            out.headers().add(HEADER_LAST_ERROR, truncate(error).getBytes(StandardCharsets.UTF_8));
        }
        // Capture the async outcome: flush() waits for delivery but swallows failures, so the
        // drain loop re-checks publishFailure before committing the consumer offset.
        producer.send(out, (metadata, exception) -> {
            if (exception != null) {
                publishFailure.compareAndSet(null, exception);
            }
        });
    }

    private static boolean isReprocessHeader(String key) {
        return HEADER_ATTEMPTS.equals(key)
                || HEADER_ORIGINAL_TOPIC.equals(key)
                || HEADER_LAST_ATTEMPT_AT.equals(key)
                || HEADER_LAST_ERROR.equals(key);
    }

    private static int readAttempts(ConsumerRecord<String, byte[]> record) {
        Header h = record.headers().lastHeader(HEADER_ATTEMPTS);
        if (h == null || h.value() == null) {
            return 0;
        }
        try {
            return Math.max(0, Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8).trim()));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String truncate(String s) {
        return s.length() <= MAX_ERROR_HEADER_LEN ? s : s.substring(0, MAX_ERROR_HEADER_LEN);
    }
}
