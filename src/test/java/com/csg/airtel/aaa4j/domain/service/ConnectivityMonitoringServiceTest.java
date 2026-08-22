package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.config.ConnectivityMonitoringConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Response;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Query;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectivityMonitoringServiceTest {

    private static final int FAILURE_THRESHOLD = 3;
    private static final String SERVICE_NAME = "db-write-service";

    private MeterRegistry registry;
    private Pool dbPool;
    private ReactiveRedisDataSource redisDataSource;
    private ConnectivityMonitoringConfig config;
    private ConnectivityMonitoringService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        dbPool = mock(Pool.class);
        redisDataSource = mock(ReactiveRedisDataSource.class);
        config = mock(ConnectivityMonitoringConfig.class);
        when(config.serviceName()).thenReturn(SERVICE_NAME);
        when(config.enabled()).thenReturn(true);
        when(config.failureThreshold()).thenReturn(FAILURE_THRESHOLD);
        when(config.probeTimeoutMs()).thenReturn(500L);
        when(config.probeDatabase()).thenReturn(true);
        when(config.probeRedis()).thenReturn(true);
        when(config.probeKafka()).thenReturn(false);
        service = new ConnectivityMonitoringService(registry, dbPool, redisDataSource, config, "localhost:9092");
    }

    // ---- State machine ----

    @Test
    void startsWithEveryDependencyUp() {
        assertTrue(service.allUp());
        for (ConnectivityMonitoringService.Dependency dependency : ConnectivityMonitoringService.Dependency.values()) {
            assertTrue(service.isUp(dependency));
            assertEquals(1.0, upGauge(dependency));
        }
    }

    @Test
    void everyMeterCarriesTheServiceTag() {
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS,
                new ConnectException("Connection refused"));

        assertNotNull(registry.find("dependency_up")
                .tags(Tags.of("service", SERVICE_NAME, "dependency", "redis")).gauge());
        assertNotNull(registry.find("dependency_connectivity_failure_count")
                .tags(Tags.of("service", SERVICE_NAME, "dependency", "redis")).counter());
        assertNotNull(registry.find("dependency_outage_count")
                .tags(Tags.of("service", SERVICE_NAME, "dependency", "redis")).counter());
    }

    @Test
    void failuresBelowThresholdKeepDependencyUp() {
        for (int i = 0; i < FAILURE_THRESHOLD - 1; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS,
                    new ConnectException("Connection refused"));
        }

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
        assertEquals(1.0, upGauge(ConnectivityMonitoringService.Dependency.REDIS));
        assertEquals(FAILURE_THRESHOLD - 1.0, gauge("dependency_consecutive_failure_count", "redis"));
        assertEquals(FAILURE_THRESHOLD - 1.0,
                failureCounter("redis", ConnectivityFailureReason.CONNECTION_REFUSED).count());
    }

    @Test
    void thresholdFailuresMarkDependencyDownAndCountTheOutage() {
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.DATABASE,
                    new RuntimeException("ORA-12541: TNS:no listener"));
        }

        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.DATABASE));
        assertFalse(service.allUp());
        assertEquals(0.0, upGauge(ConnectivityMonitoringService.Dependency.DATABASE));
        assertEquals(1.0, registry.find("dependency_outage_count").tags(Tags.of("dependency", "database"))
                .counter().count());
        assertTrue(gauge("dependency_last_failure_timestamp_seconds", "database") > 0.0);
        // Other dependencies are unaffected
        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
    }

    @Test
    void successRestoresDependencyAndRecordsOutageDuration() {
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.KAFKA,
                    new RuntimeException("Topic db-write-dc not present in metadata after 60000 ms"));
        }
        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.KAFKA));

        service.recordSuccess(ConnectivityMonitoringService.Dependency.KAFKA);

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.KAFKA));
        assertEquals(1.0, upGauge(ConnectivityMonitoringService.Dependency.KAFKA));
        assertEquals(0.0, gauge("dependency_consecutive_failure_count", "kafka"));
        assertEquals(0.0, gauge("dependency_downtime_seconds", "kafka"));
        assertTrue(gauge("dependency_last_success_timestamp_seconds", "kafka") > 0.0);
        assertEquals(1L, registry.find("dependency.outage.duration").tags(Tags.of("dependency", "kafka"))
                .timer().count());
    }

    @Test
    void interleavedSuccessResetsTheFailureStreak() {
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        service.recordSuccess(ConnectivityMonitoringService.Dependency.REDIS);
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
        assertEquals(1.0, gauge("dependency_consecutive_failure_count", "redis"));
    }

    @Test
    void applicationErrorsAreCountedButNeverTakeADependencyDown() {
        for (int i = 0; i < FAILURE_THRESHOLD * 2; i++) {
            ConnectivityFailureReason reason = service.recordFailure(
                    ConnectivityMonitoringService.Dependency.DATABASE,
                    new IllegalArgumentException("table name must not be null"));
            assertEquals(ConnectivityFailureReason.APPLICATION_ERROR, reason);
        }

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.DATABASE));
        assertEquals(0.0, gauge("dependency_consecutive_failure_count", "database"));
        assertEquals(FAILURE_THRESHOLD * 2.0,
                errorCounter("database", ConnectivityFailureReason.APPLICATION_ERROR).count());
        // ...and no connectivity failure series was created for it
        assertEquals(0, registry.find("dependency_connectivity_failure_count")
                .tags(Tags.of("dependency", "database")).counters().size());
    }

    @Test
    void nullArgumentsAreIgnored() {
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR, service.recordFailure(null, new ConnectException()));
        assertEquals(ConnectivityFailureReason.APPLICATION_ERROR,
                service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, null));
        service.recordSuccess(null);
        assertTrue(service.allUp());
    }

    // ---- Probes ----

    @Test
    void databaseProbeSuccessMarksDependencyUpAgain() {
        stubDatabaseProbe(Uni.createFrom().item(mock(RowSet.class)));
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.DATABASE, new ConnectException("Connection refused"));
        }
        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.DATABASE));

        service.probeDatabase();

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.DATABASE));
        assertNotNull(registry.find("dependency.probe.latency")
                .tags(Tags.of("dependency", "database", "outcome", "success")).timer());
    }

    @Test
    void databaseProbeFailureCountsTowardsTheOutage() {
        stubDatabaseProbe(Uni.createFrom().failure(new ConnectException("Connection refused")));

        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.probeDatabase();
        }

        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.DATABASE));
        assertEquals(FAILURE_THRESHOLD * 1.0,
                failureCounter("database", ConnectivityFailureReason.CONNECTION_REFUSED).count());
        assertNotNull(registry.find("dependency.probe.latency")
                .tags(Tags.of("dependency", "database", "outcome", "failure")).timer());
    }

    @Test
    void probeFailureWithoutARecognisableCauseStillCountsAsUnavailable() {
        stubDatabaseProbe(Uni.createFrom().failure(new IllegalStateException("probe could not run")));

        service.probeDatabase();

        assertEquals(1.0, failureCounter("database", ConnectivityFailureReason.SERVICE_UNAVAILABLE).count());
        assertEquals(1.0, gauge("dependency_consecutive_failure_count", "database"));
    }

    @Test
    void probeTreatsAnUnusableClientAsAnOutageRatherThanThrowing() {
        // e.g. the datasource never started, so the injected pool is unusable.
        when(dbPool.query(anyString())).thenThrow(new IllegalStateException("Pool not started"));

        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.probeDatabase();
        }

        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.DATABASE));
        assertEquals(FAILURE_THRESHOLD * 1.0,
                failureCounter("database", ConnectivityFailureReason.SERVICE_UNAVAILABLE).count());
    }

    @Test
    void redisProbeDrivesRedisState() {
        when(redisDataSource.execute("PING")).thenReturn(Uni.createFrom().failure(new ConnectException("Connection refused")));

        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.probeRedis();
        }
        assertFalse(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));

        when(redisDataSource.execute("PING")).thenReturn(Uni.createFrom().item(mock(Response.class)));
        service.probeRedis();

        assertTrue(service.isUp(ConnectivityMonitoringService.Dependency.REDIS));
    }

    @Test
    void schedulerProbesOnlyEnabledDependencies() {
        stubDatabaseProbe(Uni.createFrom().item(mock(RowSet.class)));
        when(redisDataSource.execute("PING")).thenReturn(Uni.createFrom().item(mock(Response.class)));
        when(config.probeDatabase()).thenReturn(false);

        service.probeDependencies();

        verify(dbPool, never()).query(anyString());
        verify(redisDataSource).execute("PING");
    }

    @Test
    void schedulerDoesNothingWhenMonitoringIsDisabled() {
        when(config.enabled()).thenReturn(false);

        service.probeDependencies();

        verify(dbPool, never()).query(anyString());
        verify(redisDataSource, never()).execute(anyString());
    }

    // ---- Read model / housekeeping ----

    @Test
    void snapshotReportsPerDependencyState() {
        for (int i = 0; i < FAILURE_THRESHOLD; i++) {
            service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        }

        Map<String, ConnectivityMonitoringService.DependencyStatus> snapshot = service.snapshot();
        assertEquals(3, snapshot.size());

        ConnectivityMonitoringService.DependencyStatus redis = snapshot.get("redis");
        assertFalse(redis.up());
        assertEquals(FAILURE_THRESHOLD, redis.consecutiveFailures());
        assertEquals(FAILURE_THRESHOLD, redis.connectivityFailureCount());
        assertEquals(FAILURE_THRESHOLD, redis.dailyConnectivityFailureCount());
        assertEquals(1L, redis.outageCount());
        assertEquals(ConnectivityFailureReason.CONNECTION_REFUSED.label(), redis.lastFailureReason());
        assertTrue(redis.lastFailureEpochSeconds() > 0);

        assertTrue(snapshot.get("database").up());
        assertEquals(0L, snapshot.get("database").connectivityFailureCount());
    }

    @Test
    void dailyResetClearsDailyCountsButKeepsLifetimeCounts() {
        service.recordFailure(ConnectivityMonitoringService.Dependency.REDIS, new ConnectException("Connection refused"));
        assertEquals(1.0, gauge("dependency_connectivity_failure_daily_count", "redis"));

        service.resetDailyCounters();

        assertEquals(0.0, gauge("dependency_connectivity_failure_daily_count", "redis"));
        assertEquals(1.0, failureCounter("redis", ConnectivityFailureReason.CONNECTION_REFUSED).count());
        assertEquals(1L, service.snapshot().get("redis").connectivityFailureCount());
    }

    // ---- Helpers ----

    @SuppressWarnings("unchecked")
    private void stubDatabaseProbe(Uni<RowSet<Row>> result) {
        Query<RowSet<Row>> query = mock(Query.class);
        when(query.execute()).thenReturn(result);
        when(dbPool.query(anyString())).thenReturn(query);
    }

    private double upGauge(ConnectivityMonitoringService.Dependency dependency) {
        return gauge("dependency_up", dependency.label());
    }

    private double gauge(String name, String dependency) {
        Gauge g = registry.find(name).tags(Tags.of("dependency", dependency)).gauge();
        assertNotNull(g, "gauge not registered: " + name + "{dependency=" + dependency + "}");
        return g.value();
    }

    private Counter failureCounter(String dependency, ConnectivityFailureReason reason) {
        Counter counter = registry.find("dependency_connectivity_failure_count")
                .tags(Tags.of("dependency", dependency, "reason", reason.label())).counter();
        assertNotNull(counter, "counter not registered for " + dependency + "/" + reason.label());
        return counter;
    }

    private Counter errorCounter(String dependency, ConnectivityFailureReason reason) {
        Counter counter = registry.find("dependency_error_count")
                .tags(Tags.of("dependency", dependency, "reason", reason.label())).counter();
        assertNotNull(counter, "counter not registered for " + dependency + "/" + reason.label());
        return counter;
    }
}
