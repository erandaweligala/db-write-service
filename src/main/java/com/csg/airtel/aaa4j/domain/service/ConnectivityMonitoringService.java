package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.application.config.ConnectivityMonitoringConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.scheduler.Scheduled;
import io.vertx.mutiny.sqlclient.Pool;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks reachability of the three infrastructure dependencies the DB write service
 * cannot run without — Oracle, Redis, and Kafka — and exposes the result to Prometheus
 * so Grafana can chart and alert on it.
 *
 * <p>Two independent signals feed the same state machine:</p>
 * <ul>
 *   <li><b>Live traffic.</b> Every exception recorded against a dependency is routed here
 *       by {@link ExceptionMetricsService}, classified by {@link ConnectivityFailureClassifier},
 *       and — when it is a transport failure rather than an application error — counted
 *       towards that dependency's consecutive-failure streak.</li>
 *   <li><b>Active probes.</b> A scheduler pings each dependency on a fixed interval
 *       ({@code connectivity.probe-interval}), so an outage is visible in Grafana even
 *       when no traffic is flowing, and recovery is detected without waiting for a
 *       successful request.</li>
 * </ul>
 *
 * <p>A dependency flips to DOWN after {@code connectivity.failure-threshold} consecutive
 * connectivity failures and back to UP on the first success. Both transitions are logged
 * and reflected in {@code dependency_up}, alongside outage counts, downtime, per-reason
 * failure counters, and probe latency.</p>
 *
 * <h2>Metrics</h2>
 * <table>
 *   <caption>Meters registered by this service</caption>
 *   <tr><td>{@code dependency_up}</td><td>gauge, 1 = reachable, 0 = down</td></tr>
 *   <tr><td>{@code dependency_connectivity_failure_count_total}</td><td>counter by {@code reason}</td></tr>
 *   <tr><td>{@code dependency_error_count_total}</td><td>counter of all failures, connectivity or not</td></tr>
 *   <tr><td>{@code dependency_connectivity_failure_daily_count}</td><td>gauge, resets at 00:00</td></tr>
 *   <tr><td>{@code dependency_consecutive_failure_count}</td><td>gauge, current failure streak</td></tr>
 *   <tr><td>{@code dependency_outage_count_total}</td><td>counter of UP&rarr;DOWN transitions</td></tr>
 *   <tr><td>{@code dependency_downtime_seconds}</td><td>gauge, length of the outage in progress</td></tr>
 *   <tr><td>{@code dependency_last_failure_timestamp_seconds}</td><td>gauge, epoch seconds</td></tr>
 *   <tr><td>{@code dependency_last_success_timestamp_seconds}</td><td>gauge, epoch seconds</td></tr>
 *   <tr><td>{@code dependency_outage_duration_seconds}</td><td>timer, one record per recovery</td></tr>
 *   <tr><td>{@code dependency_probe_latency_seconds}</td><td>timer by {@code outcome}</td></tr>
 * </table>
 */
@ApplicationScoped
public class ConnectivityMonitoringService {

    private static final Logger log = Logger.getLogger(ConnectivityMonitoringService.class);
    private static final String M_INIT = "init";
    private static final String M_FAILURE = "recordFailure";
    private static final String M_SUCCESS = "recordSuccess";
    private static final String M_PROBE = "probeDependencies";
    private static final String M_RESET = "dailyReset";
    private static final String M_CLOSE = "close";

    private static final String METRIC_UP = "dependency_up";
    private static final String METRIC_CONNECTIVITY_FAILURE = "dependency_connectivity_failure_count";
    private static final String METRIC_ERROR = "dependency_error_count";
    private static final String METRIC_FAILURE_DAILY = "dependency_connectivity_failure_daily_count";
    private static final String METRIC_CONSECUTIVE = "dependency_consecutive_failure_count";
    private static final String METRIC_OUTAGE = "dependency_outage_count";
    private static final String METRIC_DOWNTIME = "dependency_downtime_seconds";
    private static final String METRIC_LAST_FAILURE = "dependency_last_failure_timestamp_seconds";
    private static final String METRIC_LAST_SUCCESS = "dependency_last_success_timestamp_seconds";
    private static final String METRIC_OUTAGE_DURATION = "dependency.outage.duration";
    private static final String METRIC_PROBE_LATENCY = "dependency.probe.latency";

    private static final String TAG_SERVICE = "service";
    private static final String TAG_DEPENDENCY = "dependency";
    private static final String TAG_REASON = "reason";
    private static final String TAG_OUTCOME = "outcome";
    private static final String OUTCOME_SUCCESS = "success";
    private static final String OUTCOME_FAILURE = "failure";

    /** Cheapest round-trip that proves the Oracle session is alive. */
    private static final String DB_PROBE_QUERY = "SELECT 1 FROM DUAL";
    private static final String REDIS_PROBE_COMMAND = "PING";
    private static final String KAFKA_PROBE_CLIENT_ID = "db-write-service-connectivity-probe";

    private static final int MILLIS_PER_SECOND = 1000;

    /** The infrastructure dependencies whose reachability is tracked. */
    public enum Dependency {
        DATABASE("database"),
        REDIS("redis"),
        KAFKA("kafka");

        private final String label;

        Dependency(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private final MeterRegistry registry;
    private final Pool dbPool;
    private final ReactiveRedisDataSource redisDataSource;
    private final ConnectivityMonitoringConfig config;
    private final String kafkaBootstrapServers;
    private final Duration probeTimeout;
    /** Distinguishes this service's series from the identically named ones exported by its siblings. */
    private final String serviceName;

    private final Map<Dependency, DependencyState> states = new EnumMap<>(Dependency.class);

    /** Created on the first Kafka probe so a broker outage at boot does not block startup. */
    private volatile AdminClient kafkaAdminClient;

    @Inject
    public ConnectivityMonitoringService(
            MeterRegistry registry,
            Pool dbPool,
            ReactiveRedisDataSource redisDataSource,
            ConnectivityMonitoringConfig config,
            @ConfigProperty(name = "kafka.bootstrap.servers", defaultValue = "localhost:9092")
            String kafkaBootstrapServers) {
        this.registry = registry;
        this.dbPool = dbPool;
        this.redisDataSource = redisDataSource;
        this.config = config;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.probeTimeout = Duration.ofMillis(config.probeTimeoutMs());
        this.serviceName = config.serviceName();

        for (Dependency dependency : Dependency.values()) {
            states.put(dependency, new DependencyState(dependency, serviceName, registry));
        }

        LoggingUtil.logInfo(log, M_INIT,
                "ConnectivityMonitoringService initialized. service=%s, enabled=%s, failureThreshold=%d, probeTimeoutMs=%d, kafkaBootstrap=%s",
                serviceName, config.enabled(), config.failureThreshold(), config.probeTimeoutMs(), kafkaBootstrapServers);
    }

    // ---- Failure / success recording ----

    /**
     * Records a failure observed while talking to {@code dependency}.
     *
     * <p>Every failure increments {@code dependency_error_count}. Only failures the
     * classifier recognises as transport problems increment the connectivity counters
     * and the consecutive-failure streak that can flip the dependency to DOWN.</p>
     *
     * @param dependency the dependency the call was made against; {@code null} is ignored
     * @param throwable  the failure; {@code null} is ignored
     * @return the classified reason, {@link ConnectivityFailureReason#APPLICATION_ERROR} when it is not a connectivity problem
     */
    public ConnectivityFailureReason recordFailure(Dependency dependency, Throwable throwable) {
        if (dependency == null || throwable == null) {
            return ConnectivityFailureReason.APPLICATION_ERROR;
        }
        try {
            ConnectivityFailureReason reason = ConnectivityFailureClassifier.classify(throwable);
            DependencyState state = states.get(dependency);
            state.errorCounter(registry, reason).increment();

            if (!reason.isConnectivityFailure()) {
                return reason;
            }
            applyConnectivityFailure(state, reason, throwable);
            return reason;
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_FAILURE, "Failed to record connectivity failure for %s: %s",
                    dependency.label(), e.getMessage());
            return ConnectivityFailureReason.APPLICATION_ERROR;
        }
    }

    /**
     * Records a successful interaction with {@code dependency}: clears the failure
     * streak and, if the dependency was DOWN, marks it recovered.
     *
     * @param dependency the dependency that answered; {@code null} is ignored
     */
    public void recordSuccess(Dependency dependency) {
        if (dependency == null) {
            return;
        }
        try {
            DependencyState state = states.get(dependency);
            long nowMillis = System.currentTimeMillis();
            state.consecutiveFailures.set(0);
            state.lastSuccessEpochSeconds.set(nowMillis / MILLIS_PER_SECOND);

            if (state.up.compareAndSet(0L, 1L)) {
                long downSince = state.downSinceEpochMillis.getAndSet(0L);
                long outageMillis = downSince > 0 ? nowMillis - downSince : 0L;
                state.outageDurationTimer.record(outageMillis, TimeUnit.MILLISECONDS);
                LoggingUtil.logInfo(log, M_SUCCESS,
                        "Connectivity RESTORED for %s after %d ms of downtime",
                        dependency.label(), outageMillis);
            }
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_SUCCESS, "Failed to record connectivity success for %s: %s",
                    dependency.label(), e.getMessage());
        }
    }

    private void applyConnectivityFailure(DependencyState state, ConnectivityFailureReason reason, Throwable throwable) {
        long nowMillis = System.currentTimeMillis();
        state.connectivityFailureCounter(registry, reason).increment();
        state.connectivityFailureTotal.incrementAndGet();
        state.dailyConnectivityFailureCount.incrementAndGet();
        state.lastFailureEpochSeconds.set(nowMillis / MILLIS_PER_SECOND);
        state.lastReason = reason;

        int consecutive = state.consecutiveFailures.incrementAndGet();
        if (consecutive >= config.failureThreshold() && state.up.compareAndSet(1L, 0L)) {
            state.downSinceEpochMillis.set(nowMillis);
            state.outageCounter.increment();
            LoggingUtil.logError(log, M_FAILURE, throwable,
                    "Connectivity DOWN for %s after %d consecutive failures, reason=%s",
                    state.dependency.label(), consecutive, reason.label());
        } else {
            LoggingUtil.logWarn(log, M_FAILURE,
                    "Connectivity failure for %s, reason=%s, consecutiveFailures=%d, cause=%s",
                    state.dependency.label(), reason.label(), consecutive, throwable.toString());
        }
    }

    // ---- Active probes ----

    /**
     * Pings every enabled dependency so outages and recoveries surface in Grafana
     * regardless of traffic. Overlapping runs are skipped — a probe that is still
     * waiting on a dead dependency must not stack up behind itself.
     */
    @Scheduled(every = "{connectivity.probe-interval:15s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void probeDependencies() {
        if (!config.enabled()) {
            return;
        }
        if (config.probeDatabase()) {
            probeDatabase();
        }
        if (config.probeRedis()) {
            probeRedis();
        }
        if (config.probeKafka()) {
            probeKafka();
        }
    }

    /** Oracle probe: a single {@code SELECT 1 FROM DUAL} round trip, bounded by the probe timeout. */
    void probeDatabase() {
        long startNanos = System.nanoTime();
        try {
            dbPool.query(DB_PROBE_QUERY).execute()
                    .ifNoItem().after(probeTimeout).fail()
                    .subscribe().with(
                            rows -> onProbeSuccess(Dependency.DATABASE, startNanos),
                            error -> onProbeFailure(Dependency.DATABASE, startNanos, error));
        } catch (Exception e) {
            // The client itself is unusable (pool not started, datasource inactive) — that is
            // an outage as far as this service is concerned, not something to throw at the scheduler.
            onProbeFailure(Dependency.DATABASE, startNanos, e);
        }
    }

    /** Redis probe: {@code PING}, bounded by the probe timeout. */
    void probeRedis() {
        long startNanos = System.nanoTime();
        try {
            redisDataSource.execute(REDIS_PROBE_COMMAND)
                    .ifNoItem().after(probeTimeout).fail()
                    .subscribe().with(
                            response -> onProbeSuccess(Dependency.REDIS, startNanos),
                            error -> onProbeFailure(Dependency.REDIS, startNanos, error));
        } catch (Exception e) {
            onProbeFailure(Dependency.REDIS, startNanos, e);
        }
    }

    /**
     * Kafka probe: {@code describeCluster} through an AdminClient. Blocking by nature —
     * safe here because scheduled methods run on a worker thread, and bounded by the
     * probe timeout on both the request and the future.
     */
    void probeKafka() {
        long startNanos = System.nanoTime();
        try {
            AdminClient admin = kafkaAdminClient();
            admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) config.probeTimeoutMs()))
                    .nodes()
                    .get(config.probeTimeoutMs(), TimeUnit.MILLISECONDS);
            onProbeSuccess(Dependency.KAFKA, startNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            onProbeFailure(Dependency.KAFKA, startNanos, e);
        } catch (Exception e) {
            onProbeFailure(Dependency.KAFKA, startNanos, e);
        }
    }

    private AdminClient kafkaAdminClient() {
        AdminClient client = kafkaAdminClient;
        if (client != null) {
            return client;
        }
        synchronized (this) {
            if (kafkaAdminClient == null) {
                int timeoutMs = (int) config.probeTimeoutMs();
                Properties props = new Properties();
                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
                props.put(AdminClientConfig.CLIENT_ID_CONFIG, KAFKA_PROBE_CLIENT_ID);
                props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
                props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs * 2);
                props.put(AdminClientConfig.RETRIES_CONFIG, 0);
                kafkaAdminClient = AdminClient.create(props);
                LoggingUtil.logInfo(log, M_PROBE, "Kafka connectivity probe client created for bootstrap servers: %s",
                        kafkaBootstrapServers);
            }
            return kafkaAdminClient;
        }
    }

    private void onProbeSuccess(Dependency dependency, long startNanos) {
        recordProbeLatency(dependency, startNanos, OUTCOME_SUCCESS);
        recordSuccess(dependency);
        LoggingUtil.logDebug(log, M_PROBE, "Connectivity probe succeeded for %s", dependency.label());
    }

    private void onProbeFailure(Dependency dependency, long startNanos, Throwable error) {
        try {
            recordProbeLatency(dependency, startNanos, OUTCOME_FAILURE);
            // A probe that cannot complete is a connectivity failure by definition, even when
            // the classifier cannot name the transport fault behind it.
            ConnectivityFailureReason reason = ConnectivityFailureClassifier.classify(error);
            if (!reason.isConnectivityFailure()) {
                reason = ConnectivityFailureReason.SERVICE_UNAVAILABLE;
            }
            DependencyState state = states.get(dependency);
            state.errorCounter(registry, reason).increment();
            applyConnectivityFailure(state, reason, error);
        } catch (Exception e) {
            LoggingUtil.logWarn(log, M_PROBE, "Failed to record probe failure for %s: %s",
                    dependency.label(), e.getMessage());
        }
    }

    private void recordProbeLatency(Dependency dependency, long startNanos, String outcome) {
        Timer.builder(METRIC_PROBE_LATENCY)
                .description("Latency of the connectivity probe against each dependency")
                .tag(TAG_SERVICE, serviceName)
                .tag(TAG_DEPENDENCY, dependency.label())
                .tag(TAG_OUTCOME, outcome)
                .register(registry)
                .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    /** Resets the 24-hour connectivity failure counters at midnight; lifetime counters are untouched. */
    @Scheduled(cron = "0 0 0 * * ?")
    void resetDailyCounters() {
        for (DependencyState state : states.values()) {
            LoggingUtil.logInfo(log, M_RESET, "Resetting daily connectivity failure count for %s. Previous count: %d",
                    state.dependency.label(), state.dailyConnectivityFailureCount.get());
            state.dailyConnectivityFailureCount.set(0);
        }
    }

    @PreDestroy
    void close() {
        AdminClient client = kafkaAdminClient;
        if (client != null) {
            try {
                client.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                LoggingUtil.logWarn(log, M_CLOSE, "Failed to close Kafka connectivity probe client: %s", e.getMessage());
            }
        }
    }

    // ---- Read model ----

    /** Immutable view of one dependency's connectivity state, used by the REST endpoint and tests. */
    public record DependencyStatus(
            String dependency,
            boolean up,
            int consecutiveFailures,
            long connectivityFailureCount,
            long dailyConnectivityFailureCount,
            long outageCount,
            long downtimeSeconds,
            long lastFailureEpochSeconds,
            long lastSuccessEpochSeconds,
            String lastFailureReason) {}

    /** Snapshot of every tracked dependency, keyed by label, in declaration order. */
    public Map<String, DependencyStatus> snapshot() {
        Map<String, DependencyStatus> out = LinkedHashMap.newLinkedHashMap(states.size());
        for (DependencyState state : states.values()) {
            out.put(state.dependency.label(), state.toStatus());
        }
        return Collections.unmodifiableMap(out);
    }

    /** {@code true} when the dependency is currently considered reachable. */
    public boolean isUp(Dependency dependency) {
        return dependency != null && states.get(dependency).up.get() == 1L;
    }

    /** {@code true} when every tracked dependency is reachable. */
    public boolean allUp() {
        for (DependencyState state : states.values()) {
            if (state.up.get() != 1L) {
                return false;
            }
        }
        return true;
    }

    /**
     * Per-dependency state plus the meters that expose it. Counters are created lazily
     * per reason so Prometheus only carries series that actually occurred.
     */
    private static final class DependencyState {

        private final Dependency dependency;
        private final String serviceName;
        private final AtomicLong up = new AtomicLong(1L);
        private final AtomicInteger consecutiveFailures = new AtomicInteger();
        private final AtomicLong dailyConnectivityFailureCount = new AtomicLong();
        private final AtomicLong lastFailureEpochSeconds = new AtomicLong();
        private final AtomicLong lastSuccessEpochSeconds = new AtomicLong();
        private final AtomicLong downSinceEpochMillis = new AtomicLong();
        private final AtomicLong connectivityFailureTotal = new AtomicLong();

        private final Counter outageCounter;
        private final Timer outageDurationTimer;
        private final ConcurrentMap<ConnectivityFailureReason, Counter> connectivityFailureCounters =
                new ConcurrentHashMap<>();
        private final ConcurrentMap<ConnectivityFailureReason, Counter> errorCounters = new ConcurrentHashMap<>();

        private volatile ConnectivityFailureReason lastReason;

        private DependencyState(Dependency dependency, String serviceName, MeterRegistry registry) {
            this.dependency = dependency;
            this.serviceName = serviceName;
            String label = dependency.label();

            Gauge.builder(METRIC_UP, up, AtomicLong::get)
                    .description("Whether the dependency is currently reachable (1 = up, 0 = down)")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);

            Gauge.builder(METRIC_CONSECUTIVE, consecutiveFailures, AtomicInteger::get)
                    .description("Consecutive connectivity failures since the last success")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);

            Gauge.builder(METRIC_FAILURE_DAILY, dailyConnectivityFailureCount, AtomicLong::get)
                    .description("Connectivity failures in the current 24-hour window (resets at 00:00)")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);

            Gauge.builder(METRIC_LAST_FAILURE, lastFailureEpochSeconds, AtomicLong::get)
                    .description("Epoch seconds of the most recent connectivity failure (0 = none since startup)")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);

            Gauge.builder(METRIC_LAST_SUCCESS, lastSuccessEpochSeconds, AtomicLong::get)
                    .description("Epoch seconds of the most recent successful interaction (0 = none since startup)")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);

            Gauge.builder(METRIC_DOWNTIME, this, DependencyState::currentDowntimeSeconds)
                    .description("Duration in seconds of the outage currently in progress (0 when up)")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);

            this.outageCounter = Counter.builder(METRIC_OUTAGE)
                    .description("Number of times the dependency transitioned from up to down")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);

            this.outageDurationTimer = Timer.builder(METRIC_OUTAGE_DURATION)
                    .description("Duration of each completed outage, recorded on recovery")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, label)
                    .register(registry);
        }

        private Counter connectivityFailureCounter(MeterRegistry registry, ConnectivityFailureReason reason) {
            return connectivityFailureCounters.computeIfAbsent(reason, r -> Counter.builder(METRIC_CONNECTIVITY_FAILURE)
                    .description("Connectivity failures against the dependency, broken down by reason")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, dependency.label())
                    .tag(TAG_REASON, r.label())
                    .register(registry));
        }

        private Counter errorCounter(MeterRegistry registry, ConnectivityFailureReason reason) {
            return errorCounters.computeIfAbsent(reason, r -> Counter.builder(METRIC_ERROR)
                    .description("All failures against the dependency, connectivity related or not")
                    .tag(TAG_SERVICE, serviceName)
                    .tag(TAG_DEPENDENCY, dependency.label())
                    .tag(TAG_REASON, r.label())
                    .register(registry));
        }

        private double currentDowntimeSeconds() {
            long downSince = downSinceEpochMillis.get();
            if (up.get() == 1L || downSince <= 0L) {
                return 0.0;
            }
            return (System.currentTimeMillis() - downSince) / (double) MILLIS_PER_SECOND;
        }

        private DependencyStatus toStatus() {
            ConnectivityFailureReason reason = lastReason;
            return new DependencyStatus(
                    dependency.label(),
                    up.get() == 1L,
                    consecutiveFailures.get(),
                    connectivityFailureTotal.get(),
                    dailyConnectivityFailureCount.get(),
                    (long) outageCounter.count(),
                    (long) currentDowntimeSeconds(),
                    lastFailureEpochSeconds.get(),
                    lastSuccessEpochSeconds.get(),
                    reason == null ? null : reason.label());
        }
    }
}
