package com.csg.airtel.aaa4j.infrastructure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance metrics for monitoring 1000+ TPS throughput
 * Tracks DB operations, batch processing, and error rates
 */
@ApplicationScoped
public class PerformanceMetrics {

    private final Counter dbUpdateCounter;
    private final Counter dbUpdateFailureCounter;
    private final Counter batchUpdateCounter;
    private final Timer dbUpdateTimer;
    private final Timer batchUpdateTimer;
    private final AtomicLong lastThroughputCheck = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong messagesProcessedSinceLastCheck = new AtomicLong(0);

    @Inject
    public PerformanceMetrics(MeterRegistry registry) {
        // Counter for successful DB updates
        this.dbUpdateCounter = Counter.builder("db.updates.total")
                .description("Total number of database updates")
                .tag("type", "single")
                .register(registry);

        // Counter for failed DB updates
        this.dbUpdateFailureCounter = Counter.builder("db.updates.failures")
                .description("Total number of failed database updates")
                .tag("type", "error")
                .register(registry);

        // Counter for batch updates
        this.batchUpdateCounter = Counter.builder("db.batch.updates.total")
                .description("Total number of batch database updates")
                .tag("type", "batch")
                .register(registry);

        // Timer for DB update operations
        this.dbUpdateTimer = Timer.builder("db.update.duration")
                .description("Database update operation duration")
                .tag("operation", "update")
                .register(registry);

        // Timer for batch update operations
        this.batchUpdateTimer = Timer.builder("db.batch.update.duration")
                .description("Batch database update operation duration")
                .tag("operation", "batch_update")
                .register(registry);
    }

    /**
     * Record a successful single DB update
     */
    public void recordDbUpdate() {
        dbUpdateCounter.increment();
        messagesProcessedSinceLastCheck.incrementAndGet();
    }

    /**
     * Record a failed DB update
     */
    public void recordDbUpdateFailure() {
        dbUpdateFailureCounter.increment();
    }

    /**
     * Record a batch update with count
     */
    public void recordBatchUpdate(int batchSize) {
        batchUpdateCounter.increment(batchSize);
        messagesProcessedSinceLastCheck.addAndGet(batchSize);
    }

    /**
     * Record DB update duration
     */
    public void recordDbUpdateDuration(Duration duration) {
        dbUpdateTimer.record(duration);
    }

    /**
     * Record batch update duration
     */
    public void recordBatchUpdateDuration(Duration duration) {
        batchUpdateTimer.record(duration);
    }

    /**
     * Calculate current throughput (TPS)
     */
    public double getCurrentThroughput() {
        long now = System.currentTimeMillis();
        long lastCheck = lastThroughputCheck.get();
        long elapsedMs = now - lastCheck;

        if (elapsedMs < 1000) {  // Less than 1 second, return 0
            return 0.0;
        }

        long messagesProcessed = messagesProcessedSinceLastCheck.getAndSet(0);
        lastThroughputCheck.set(now);

        return (messagesProcessed * 1000.0) / elapsedMs;
    }

    /**
     * Get total updates processed
     */
    public double getTotalUpdates() {
        return dbUpdateCounter.count() + batchUpdateCounter.count();
    }

    /**
     * Get failure rate
     */
    public double getFailureRate() {
        double total = getTotalUpdates();
        if (total == 0) return 0.0;
        return (dbUpdateFailureCounter.count() / total) * 100.0;
    }
}
