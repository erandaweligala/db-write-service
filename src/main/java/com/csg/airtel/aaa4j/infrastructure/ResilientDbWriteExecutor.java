package com.csg.airtel.aaa4j.infrastructure;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;

/**
 * Wraps {@link DBWriteService} with the resilience policy required by the Kafka consumer
 * pipeline so that no message is lost on DB failure:
 *
 *  1. Circuit-breaker gate     – if the breaker is OPEN we short-circuit the call with a
 *                                {@link CircuitOpenException} (classified as transient).
 *  2. In-app retry with backoff – transient DB errors (connection / timeout / deadlock /
 *                                breaker-open) are retried up to {@code retry.attempts} times
 *                                with exponential backoff. Most blips are absorbed here.
 *  3. Failure propagation       – terminal failures (permanent OR retries exhausted) are
 *                                propagated to the caller. Smallrye Reactive Messaging then
 *                                routes the original Kafka record to the dead-letter topic
 *                                (failure-strategy: dead-letter-queue) so the payload is
 *                                preserved for replay / triage. Nothing is silently dropped.
 *  4. Breaker bookkeeping       – every transient failure increments the breaker; every
 *                                success resets it. Permanent failures (constraint, bad data)
 *                                do NOT trip the breaker because they don't reflect DB health.
 */
@ApplicationScoped
public class ResilientDbWriteExecutor {

    private static final Logger log = Logger.getLogger(ResilientDbWriteExecutor.class);

    private final DBWriteService dbWriteService;
    private final DatabaseCircuitBreaker circuitBreaker;
    private final KafkaErrorMetrics metrics;

    @ConfigProperty(name = "db.retry.attempts", defaultValue = "5")
    int retryAttempts;

    @ConfigProperty(name = "db.retry.initial-backoff-ms", defaultValue = "200")
    long initialBackoffMs;

    @ConfigProperty(name = "db.retry.max-backoff-ms", defaultValue = "5000")
    long maxBackoffMs;

    @Inject
    public ResilientDbWriteExecutor(DBWriteService dbWriteService,
                                    DatabaseCircuitBreaker circuitBreaker,
                                    KafkaErrorMetrics metrics) {
        this.dbWriteService = dbWriteService;
        this.circuitBreaker = circuitBreaker;
        this.metrics = metrics;
    }

    /**
     * Execute a DB write with full resilience policy.
     *
     * The returned Uni only fails for terminal errors that must be sent to the DLT.
     * Transient errors fail the Uni only after all retries are exhausted.
     *
     * @param channel the Smallrye channel name, used to tag Prometheus metrics so
     *                dashboards can distinguish failure rates per topic.
     */
    public Uni<Void> execute(String channel, DBWriteRequest request) {
        return Uni.createFrom().deferred(() -> {
                    if (!circuitBreaker.allowRequest()) {
                        return Uni.createFrom().failure(
                                new CircuitOpenException("DB circuit breaker is OPEN — short-circuiting"));
                    }
                    return dbWriteService.processDbWriteRequest(request);
                })
                .onItem().invoke(() -> {
                    circuitBreaker.recordSuccess();
                    metrics.recordSuccess(channel);
                })
                .onFailure().invoke(t -> {
                    boolean transientFailure = KafkaFailureClassifier.isTransient(t);
                    metrics.recordFailure(channel, transientFailure);
                    if (transientFailure) {
                        circuitBreaker.recordFailure();
                        // Every transient failure on this path will trigger a retry below
                        // (until atMost is reached); count it so retries_total reflects
                        // attempted retries, not just the final outcome.
                        metrics.recordRetry(channel);
                    }
                })
                .onFailure(KafkaFailureClassifier::isTransient)
                .retry()
                .withBackOff(Duration.ofMillis(initialBackoffMs), Duration.ofMillis(maxBackoffMs))
                .withJitter(0.2)
                .atMost(retryAttempts)
                .onFailure().invoke(t -> {
                    boolean transientFailure = KafkaFailureClassifier.isTransient(t);
                    metrics.recordDltRouted(channel, transientFailure);
                    if (transientFailure) {
                        LoggingUtil.logError(log, "execute", t,
                                "DB write FAILED after %d retries — message will go to DLT (user=%s, table=%s, eventType=%s)",
                                retryAttempts, request.getUserName(), request.getTableName(), request.getEventType());
                    } else {
                        LoggingUtil.logError(log, "execute", t,
                                "DB write PERMANENT failure — message will go to DLT (user=%s, table=%s, eventType=%s)",
                                request.getUserName(), request.getTableName(), request.getEventType());
                    }
                });
    }
}
