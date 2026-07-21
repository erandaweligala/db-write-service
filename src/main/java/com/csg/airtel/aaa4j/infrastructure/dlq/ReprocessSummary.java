package com.csg.airtel.aaa4j.infrastructure.dlq;

/**
 * Immutable outcome of a single DLQ-topic reprocess run, suitable for a JSON response.
 *
 * <ul>
 *   <li>{@code succeeded} — replayed to the DB successfully; offset committed, record gone from the DLT.</li>
 *   <li>{@code requeued}  — replay failed but attempts remain; record re-published to the same DLT
 *       with an incremented attempt counter for a later run.</li>
 *   <li>{@code parked}    — replay failed and the attempt budget is spent (or the payload is
 *       unparseable); record moved to the parked topic for manual triage.</li>
 * </ul>
 * {@code total == succeeded + requeued + parked}.
 */
public record ReprocessSummary(
        String topic,
        Status status,
        long total,
        long succeeded,
        long requeued,
        long parked,
        long durationMs) {

    public enum Status {
        /** The drain ran to completion (topic emptied, budget/limit reached). */
        COMPLETED,
        /** Reprocessing is globally disabled via {@code dlq.reprocess.enabled=false}. */
        DISABLED,
        /** Another reprocess run holds the lock; try again shortly. */
        BUSY,
        /** The requested topic is not in the configured allow-list. */
        UNKNOWN_TOPIC,
        /** The topic exists in config but has no partitions on the broker (nothing to do). */
        NO_PARTITIONS,
        /** The run aborted on an unexpected error; partial counts may be present. */
        ERROR
    }

    public static ReprocessSummary status(String topic, Status status) {
        return new ReprocessSummary(topic, status, 0, 0, 0, 0, 0);
    }
}
