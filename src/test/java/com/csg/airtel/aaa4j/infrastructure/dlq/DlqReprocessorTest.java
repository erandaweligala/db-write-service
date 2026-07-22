package com.csg.airtel.aaa4j.infrastructure.dlq;

import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.infrastructure.DlqMetrics;
import com.csg.airtel.aaa4j.infrastructure.ResilientDbWriteExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DlqReprocessorTest {

    private static final String TOPIC = "DC-DR-DLT";
    private static final String PARKED_SUFFIX = ".PARKED";

    private ObjectMapper objectMapper;
    private SimpleMeterRegistry registry;
    private DlqMetrics dlqMetrics;
    private ResilientDbWriteExecutor executor;
    private MockProducer<String, byte[]> producer;
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        registry = new SimpleMeterRegistry();
        dlqMetrics = new DlqMetrics(registry);
        executor = mock(ResilientDbWriteExecutor.class);
        // A cluster that knows both the DLT and its parked topic, so the MockProducer can
        // assign partition 0 to re-queued / parked records without a real partitioner.
        Node node = Node.noNode();
        cluster = new Cluster("test-cluster", List.of(node),
                List.of(new PartitionInfo(TOPIC, 0, node, new Node[]{node}, new Node[]{node}),
                        new PartitionInfo(TOPIC + PARKED_SUFFIX, 0, node, new Node[]{node}, new Node[]{node})),
                Set.of(), Set.of());
        producer = new MockProducer<>(cluster, true, null, new StringSerializer(), new ByteArraySerializer());
    }

    private DlqReprocessor newReprocessor(KafkaClientFactory factory) {
        DlqReprocessor r = new DlqReprocessor(factory, executor, objectMapper, dlqMetrics);
        r.enabled = true;
        r.topics = List.of(TOPIC);
        r.groupId = "test-group";
        r.defaultMaxMessages = 500;
        r.pollTimeoutMs = 20;
        r.timeBudgetMs = 5000;
        r.maxAttempts = 3;
        r.parkedSuffix = PARKED_SUFFIX;
        r.replayTimeoutMs = 1000;
        return r;
    }

    private byte[] payload(String eventType, String table, String user) throws Exception {
        return objectMapper.writeValueAsBytes(DBWriteRequest.builder()
                .eventType(eventType).tableName(table).userName(user).build());
    }

    private ConsumerRecord<String, byte[]> record(long offset, byte[] value) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "key-" + offset, value);
    }

    private double reprocessCount(String topic, String outcome) {
        Counter c = registry.find("kafka.dlq.reprocess")
                .tag("topic", topic).tag("outcome", outcome).counter();
        return c == null ? 0.0 : c.count();
    }

    // -----------------------------------------------------------------------
    // Per-record decision logic (handleRecord)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("successful replay: SUCCEEDED, nothing re-published")
    void handleRecord_success() throws Exception {
        when(executor.execute(any())).thenReturn(Uni.createFrom().voidItem());
        DlqReprocessor r = newReprocessor(noopFactory());

        DlqReprocessor.Outcome outcome =
                r.handleRecord(record(0L, payload("INSERT", "T", "u")), TOPIC, producer);

        assertEquals(DlqReprocessor.Outcome.SUCCEEDED, outcome);
        assertTrue(producer.history().isEmpty(), "successful replay must not re-publish");
        assertEquals(1.0, reprocessCount(TOPIC, "succeeded"));
    }

    @Test
    @DisplayName("first failure with attempts remaining: REQUEUED to same topic, attempts=1")
    void handleRecord_requeue() throws Exception {
        when(executor.execute(any())).thenReturn(Uni.createFrom().failure(new RuntimeException("db down")));
        DlqReprocessor r = newReprocessor(noopFactory());

        DlqReprocessor.Outcome outcome =
                r.handleRecord(record(0L, payload("INSERT", "T", "u")), TOPIC, producer);

        assertEquals(DlqReprocessor.Outcome.REQUEUED, outcome);
        assertEquals(1, producer.history().size());
        ProducerRecord<String, byte[]> out = producer.history().get(0);
        assertEquals(TOPIC, out.topic(), "re-queue must target the same DLT");
        assertEquals("1", header(out, DlqReprocessor.HEADER_ATTEMPTS));
        assertEquals(TOPIC, header(out, DlqReprocessor.HEADER_ORIGINAL_TOPIC));
        assertNotNull(header(out, DlqReprocessor.HEADER_LAST_ERROR));
        assertEquals(1.0, reprocessCount(TOPIC, "requeued"));
    }

    @Test
    @DisplayName("failure on the final allowed attempt: PARKED to <topic>.PARKED")
    void handleRecord_parkWhenAttemptsExhausted() throws Exception {
        when(executor.execute(any())).thenReturn(Uni.createFrom().failure(new RuntimeException("still bad")));
        DlqReprocessor r = newReprocessor(noopFactory());

        // Already attempted twice; maxAttempts=3 => this attempt (3rd) parks.
        ConsumerRecord<String, byte[]> rec = record(0L, payload("INSERT", "T", "u"));
        rec.headers().add(DlqReprocessor.HEADER_ATTEMPTS, "2".getBytes(StandardCharsets.UTF_8));

        DlqReprocessor.Outcome outcome = r.handleRecord(rec, TOPIC, producer);

        assertEquals(DlqReprocessor.Outcome.PARKED, outcome);
        assertEquals(1, producer.history().size());
        ProducerRecord<String, byte[]> out = producer.history().get(0);
        assertEquals(TOPIC + PARKED_SUFFIX, out.topic());
        assertEquals("3", header(out, DlqReprocessor.HEADER_ATTEMPTS));
        assertEquals(1.0, reprocessCount(TOPIC, "parked"));
    }

    @Test
    @DisplayName("unparseable payload is parked immediately without invoking the DB")
    void handleRecord_unparseablePayloadIsParked() {
        DlqReprocessor r = newReprocessor(noopFactory());

        DlqReprocessor.Outcome outcome =
                r.handleRecord(record(0L, "{not valid json".getBytes(StandardCharsets.UTF_8)), TOPIC, producer);

        assertEquals(DlqReprocessor.Outcome.PARKED, outcome);
        assertEquals(1, producer.history().size());
        assertEquals(TOPIC + PARKED_SUFFIX, producer.history().get(0).topic());
        assertEquals(1.0, reprocessCount(TOPIC, "parked"));
    }

    @Test
    @DisplayName("re-queue preserves original headers and does not duplicate the attempts header")
    void handleRecord_preservesHeaders() throws Exception {
        when(executor.execute(any())).thenReturn(Uni.createFrom().failure(new RuntimeException("boom")));
        DlqReprocessor r = newReprocessor(noopFactory());

        ConsumerRecord<String, byte[]> rec = record(0L, payload("INSERT", "T", "u"));
        rec.headers().add("dead-letter-reason", "db_permanent".getBytes(StandardCharsets.UTF_8));
        rec.headers().add(DlqReprocessor.HEADER_ATTEMPTS, "0".getBytes(StandardCharsets.UTF_8));

        r.handleRecord(rec, TOPIC, producer);

        ProducerRecord<String, byte[]> out = producer.history().get(0);
        assertEquals("db_permanent", header(out, "dead-letter-reason"), "original DLT headers preserved");
        long attemptsHeaders = countHeaders(out, DlqReprocessor.HEADER_ATTEMPTS);
        assertEquals(1, attemptsHeaders, "the attempts header must not be duplicated");
        assertEquals("1", header(out, DlqReprocessor.HEADER_ATTEMPTS));
    }

    // -----------------------------------------------------------------------
    // Full drain loop (reprocessTopic) with a MockConsumer
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("drain replays every record that existed at start, then completes")
    void reprocessTopic_drainsAndCompletes() throws Exception {
        when(executor.execute(any())).thenReturn(Uni.createFrom().voidItem());

        TopicPartition tp = new TopicPartition(TOPIC, 0);
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, List.of(
                new PartitionInfo(TOPIC, 0, Node.noNode(), new Node[]{}, new Node[]{})));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        consumer.updateEndOffsets(Map.of(tp, 2L));

        byte[] v1 = payload("INSERT", "T", "u1");
        byte[] v2 = payload("UPDATE", "T", "u2");
        consumer.schedulePollTask(() -> {
            consumer.addRecord(record(0L, v1));
            consumer.addRecord(record(1L, v2));
        });

        DlqReprocessor r = newReprocessor(factoryFor(consumer, producer));
        ReprocessSummary summary = r.reprocessTopic(TOPIC, 0);

        assertEquals(ReprocessSummary.Status.COMPLETED, summary.status());
        assertEquals(2, summary.total());
        assertEquals(2, summary.succeeded());
        assertEquals(0, summary.requeued());
        assertEquals(0, summary.parked());
        assertTrue(producer.history().isEmpty());
    }

    @Test
    @DisplayName("failed park/re-queue send aborts the run before the offset commit — no record loss")
    void reprocessTopic_publishFailureAbortsBeforeCommit() {
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        // Record every commit, because MockConsumer refuses committed() lookups after close.
        Map<TopicPartition, OffsetAndMetadata> commits = new java.util.HashMap<>();
        MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                commits.putAll(offsets);
                super.commitSync(offsets);
            }
        };
        consumer.updatePartitions(TOPIC, List.of(
                new PartitionInfo(TOPIC, 0, Node.noNode(), new Node[]{}, new Node[]{})));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        consumer.updateEndOffsets(Map.of(tp, 1L));

        // Unparseable record => park() => send fails (e.g. parked topic missing on the broker).
        consumer.schedulePollTask(() ->
                consumer.addRecord(record(0L, "{not valid json".getBytes(StandardCharsets.UTF_8))));

        Producer<String, byte[]> failingProducer = new MockProducer<>(cluster, false, null,
                new StringSerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> rec,
                                                            Callback callback) {
                TimeoutException failure =
                        new TimeoutException("Topic " + rec.topic() + " not present in metadata");
                callback.onCompletion(null, failure);
                return CompletableFuture.failedFuture(failure);
            }
        };

        DlqReprocessor r = newReprocessor(factoryFor(consumer, failingProducer));
        ReprocessSummary summary = r.reprocessTopic(TOPIC, 0);

        assertEquals(ReprocessSummary.Status.ERROR, summary.status(),
                "a failed re-publish must abort the run");
        assertTrue(commits.isEmpty(),
                "the offset must NOT be committed when the park never landed");
    }

    @Test
    @DisplayName("failIfPublishFailed clears the failure slot so the next run starts clean")
    void failIfPublishFailed_throwsOnceThenClears() {
        DlqReprocessor r = newReprocessor(noopFactory());
        MockProducer<String, byte[]> manual = new MockProducer<>(cluster, false, null,
                new StringSerializer(), new ByteArraySerializer());

        // A park whose send fails asynchronously (callback fired via errorNext).
        r.handleRecord(record(0L, "{not valid json".getBytes(StandardCharsets.UTF_8)), TOPIC, manual);
        manual.errorNext(new TimeoutException("no such topic"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> r.failIfPublishFailed(TOPIC));
        assertTrue(ex.getMessage().contains(TOPIC + PARKED_SUFFIX),
                "the abort message should tell the operator which parked topic to create");

        // The slot is consumed by the throw — a subsequent check must pass.
        assertDoesNotThrow(() -> r.failIfPublishFailed(TOPIC));
    }

    @Test
    @DisplayName("reprocessTopic rejects a topic outside the configured allow-list")
    void reprocessTopic_unknownTopicRejected() {
        DlqReprocessor r = newReprocessor(noopFactory());
        ReprocessSummary summary = r.reprocessTopic("some-other-topic", 0);
        assertEquals(ReprocessSummary.Status.UNKNOWN_TOPIC, summary.status());
    }

    @Test
    @DisplayName("reprocessTopic reports DISABLED when the feature is turned off")
    void reprocessTopic_disabled() {
        DlqReprocessor r = newReprocessor(noopFactory());
        r.enabled = false;
        ReprocessSummary summary = r.reprocessTopic(TOPIC, 0);
        assertEquals(ReprocessSummary.Status.DISABLED, summary.status());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static String header(ProducerRecord<String, byte[]> rec, String key) {
        Header h = rec.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static long countHeaders(ProducerRecord<String, byte[]> rec, String key) {
        long n = 0;
        for (Header ignored : rec.headers().headers(key)) {
            n++;
        }
        return n;
    }

    private KafkaClientFactory noopFactory() {
        return factoryFor(null, producer);
    }

    private KafkaClientFactory factoryFor(Consumer<String, byte[]> consumer, Producer<String, byte[]> prod) {
        return new KafkaClientFactory() {
            @Override
            public Consumer<String, byte[]> createConsumer(String groupId) {
                return consumer;
            }

            @Override
            public Producer<String, byte[]> createProducer() {
                return prod;
            }
        };
    }
}
