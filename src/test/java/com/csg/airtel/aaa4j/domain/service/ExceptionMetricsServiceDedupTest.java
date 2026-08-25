package com.csg.airtel.aaa4j.domain.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the retry dedup in {@link ExceptionMetricsService}: one failed operation must be
 * one observation, however many times fault tolerance retried it, while genuinely separate
 * requests stay separate.
 */
class ExceptionMetricsServiceDedupTest {

    private SimpleMeterRegistry registry;
    private ExceptionMetricsService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        // The service null-checks both collaborators, so neither is needed to exercise dedup.
        service = new ExceptionMetricsService(registry, null, null);
        service.init();
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void retryAttemptsThatLandOnDifferentThreadsAreCountedOnce() throws Exception {
        // A retried reactive call resubscribes on a different fault-tolerance thread on every
        // attempt, and Vert.x mints a fresh context per thread, so no two attempts of one
        // logical failure share a thread or a context. @Retry(maxRetries = 3) therefore reaches
        // this service four times for a single failed operation, and must still count once.
        for (int attempt = 1; attempt <= 4; attempt++) {
            Thread thread = new Thread(() -> service.recordException(
                    new RuntimeException("connection refused"),
                    ExceptionMetricsService.Layer.REPOSITORY,
                    ExceptionMetricsService.Source.ORACLE),
                    "executor-thread-" + attempt);
            thread.start();
            thread.join();
        }

        assertEquals(1L, service.getTotalRootCount());
        Counter counter = registry.find("application_exception_count")
                .tag("exception_type", "RuntimeException")
                .tag("layer", ExceptionMetricsService.Layer.REPOSITORY.label())
                .tag("source", ExceptionMetricsService.Source.ORACLE.label())
                .counter();
        assertNotNull(counter);
        assertEquals(1.0, counter.count());
    }

    @Test
    void requestsOnTheirOwnDuplicatedContextsStayApart() throws Exception {
        // An in-flight request owns a duplicated Vert.x context that stays bound across its
        // retries, so the two requests below are two observations even though each of them
        // reaches this service three times.
        Vertx vertx = Vertx.vertx();
        try {
            recordThreeAttemptsOnANewRequestContext(vertx);
            recordThreeAttemptsOnANewRequestContext(vertx);
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }

        assertEquals(2L, service.getTotalRootCount());
    }

    private void recordThreeAttemptsOnANewRequestContext(Vertx vertx) throws Exception {
        Context requestContext = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
        CountDownLatch done = new CountDownLatch(1);
        requestContext.runOnContext(ignored -> {
            for (int attempt = 1; attempt <= 3; attempt++) {
                service.recordException(new RuntimeException("cache unavailable"),
                        ExceptionMetricsService.Layer.SERVICE,
                        ExceptionMetricsService.Source.REDIS);
            }
            done.countDown();
        });
        assertTrue(done.await(10, TimeUnit.SECONDS));
    }
}
