package com.csg.airtel.aaa4j.infrastructure.dlq;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Creates the short-lived Kafka clients the DLQ reprocessor uses to drain and
 * re-publish dead-letter records.
 *
 * <p>Abstracted behind an interface so the {@link DlqReprocessor} poll/replay logic
 * can be driven by {@code MockConsumer} / {@code MockProducer} in unit tests without
 * a live broker.
 */
public interface KafkaClientFactory {

    /**
     * A manual-commit consumer ({@code enable.auto.commit=false}) keyed by String with
     * byte[] values, so any DLT payload (JSON bytes or JSON string bytes) can be read
     * uniformly. The caller is responsible for assignment, seeking and closing.
     *
     * @param groupId consumer group used to track reprocess progress across runs
     */
    Consumer<String, byte[]> createConsumer(String groupId);

    /**
     * A producer keyed by String with byte[] values, used to re-queue records back to
     * their DLT (another attempt) or move them to the parked topic. The caller is
     * responsible for flushing and closing.
     */
    Producer<String, byte[]> createProducer();
}
