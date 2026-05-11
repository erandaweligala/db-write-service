package com.csg.airtel.aaa4j.infrastructure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Prometheus error metrics for the Kafka → DB write resilience pipeline.
 *
 * All metrics are scraped from /q/metrics in Prometheus exposition format.
 * Tags allow drilling into per-channel and per-classification behaviour so
 * dashboards / alerts can distinguish a flaky DB from a poison-message storm.
 *
 * Counter names:
 *   kafka_consumer_failures_total{channel, classification}      every failure observed by the executor
 *   kafka_consumer_retries_total{channel}                        every retry attempt scheduled
 *   kafka_consumer_dlt_routed_total{channel, classification}     terminal failure → sent to DLT
 *   kafka_consumer_deserialization_failures_total{channel}       payload could not be parsed
 *   kafka_consumer_success_total{channel}                        message processed OK (post-retry if any)
 *
 *   classification = "transient" | "permanent"
 */
@ApplicationScoped
public class KafkaErrorMetrics {

    private static final String CLASSIFICATION_TRANSIENT = "transient";
    private static final String CLASSIFICATION_PERMANENT = "permanent";

    private final MeterRegistry registry;

    // Counter lookup is hot-path; cache the resolved counters per tag combination
    // so we don't rebuild Tag objects on every event at 2000 TPS.
    private final ConcurrentMap<String, Counter> failureCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> retryCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> dltCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> deserializationCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> successCounters = new ConcurrentHashMap<>();

    @Inject
    public KafkaErrorMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordFailure(String channel, boolean transientFailure) {
        String classification = transientFailure ? CLASSIFICATION_TRANSIENT : CLASSIFICATION_PERMANENT;
        failureCounters.computeIfAbsent(channel + "|" + classification, k ->
                Counter.builder("kafka.consumer.failures")
                        .description("Failures observed while processing Kafka messages, before retry")
                        .tag("channel", channel)
                        .tag("classification", classification)
                        .register(registry)).increment();
    }

    public void recordRetry(String channel) {
        retryCounters.computeIfAbsent(channel, k ->
                Counter.builder("kafka.consumer.retries")
                        .description("Retry attempts scheduled by the resilient executor")
                        .tag("channel", channel)
                        .register(registry)).increment();
    }

    public void recordDltRouted(String channel, boolean transientFailure) {
        String classification = transientFailure ? CLASSIFICATION_TRANSIENT : CLASSIFICATION_PERMANENT;
        dltCounters.computeIfAbsent(channel + "|" + classification, k ->
                Counter.builder("kafka.consumer.dlt.routed")
                        .description("Messages routed to the dead-letter topic after terminal failure")
                        .tag("channel", channel)
                        .tag("classification", classification)
                        .register(registry)).increment();
    }

    public void recordDeserializationFailure(String channel) {
        deserializationCounters.computeIfAbsent(channel, k ->
                Counter.builder("kafka.consumer.deserialization.failures")
                        .description("Kafka payloads that could not be deserialized — routed to DLT")
                        .tag("channel", channel)
                        .register(registry)).increment();
    }

    public void recordSuccess(String channel) {
        successCounters.computeIfAbsent(channel, k ->
                Counter.builder("kafka.consumer.success")
                        .description("Kafka messages successfully processed (after any retries)")
                        .tag("channel", channel)
                        .register(registry)).increment();
    }
}
