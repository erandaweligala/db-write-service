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

    @ConfigProperty(name = "db.retry.attempts", defaultValue = "5")
    int retryAttempts;

    @ConfigProperty(name = "db.retry.initial-backoff-ms", defaultValue = "200")
    long initialBackoffMs;

    @ConfigProperty(name = "db.retry.max-backoff-ms", defaultValue = "5000")
    long maxBackoffMs;

    @Inject
    public ResilientDbWriteExecutor(DBWriteService dbWriteService,
                                    DatabaseCircuitBreaker circuitBreaker) {
        this.dbWriteService = dbWriteService;
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Execute a DB write with full resilience policy.
     *
     * The returned Uni only fails for terminal errors that must be sent to the DLT.
     * Transient errors fail the Uni only after all retries are exhausted.
     */
    public Uni<Void> execute(DBWriteRequest request) {
        return Uni.createFrom().deferred(() -> {
                    if (!circuitBreaker.allowRequest()) {
                        return Uni.createFrom().failure(
                                new CircuitOpenException("DB circuit breaker is OPEN — short-circuiting"));
                    }
                    return dbWriteService.processDbWriteRequest(request);
                })
                .onItem().invoke(circuitBreaker::recordSuccess)
                .onFailure().invoke(t -> {
                    if (KafkaFailureClassifier.isTransient(t)) {
                        circuitBreaker.recordFailure();
                    }
                })
                .onFailure(KafkaFailureClassifier::isTransient)
                .retry()
                .withBackOff(Duration.ofMillis(initialBackoffMs), Duration.ofMillis(maxBackoffMs))
                .withJitter(0.2)
                .atMost(retryAttempts)
                .onFailure().invoke(t -> {
                    if (KafkaFailureClassifier.isTransient(t)) {
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
