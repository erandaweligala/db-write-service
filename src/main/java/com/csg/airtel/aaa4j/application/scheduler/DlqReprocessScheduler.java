package com.csg.airtel.aaa4j.application.scheduler;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.infrastructure.dlq.DlqReprocessor;
import com.csg.airtel.aaa4j.infrastructure.dlq.ReprocessSummary;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * Optional periodic auto-replay of the dead-letter topics.
 *
 * <p><b>Disabled by default.</b> The scheduler fires on {@code dlq.reprocess.schedule.cron} but the
 * run is skipped unless {@code dlq.reprocess.schedule.enabled=true} (see {@link SchedulerDisabled}).
 * Manual replay via {@code POST /api/dlq/reprocess-all} is the primary path; enabling the schedule
 * is for environments that want unattended draining once the DB is known to be healthy again.
 */
@ApplicationScoped
public class DlqReprocessScheduler {

    private static final Logger log = Logger.getLogger(DlqReprocessScheduler.class);

    private final DlqReprocessor reprocessor;

    @ConfigProperty(name = "dlq.reprocess.schedule.max-messages", defaultValue = "0")
    int scheduledMaxMessages;

    @Inject
    public DlqReprocessScheduler(DlqReprocessor reprocessor) {
        this.reprocessor = reprocessor;
    }

    @Scheduled(
            cron = "{dlq.reprocess.schedule.cron}",
            identity = "dlq-reprocess",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            skipExecutionIf = SchedulerDisabled.class)
    void scheduledReprocess() {
        LoggingUtil.logInfo(log, "scheduledReprocess", "Scheduled DLQ reprocess starting");
        Map<String, ReprocessSummary> results = reprocessor.reprocessAll(scheduledMaxMessages);
        for (ReprocessSummary s : results.values()) {
            if (s.total() > 0 || s.status() != ReprocessSummary.Status.COMPLETED) {
                LoggingUtil.logInfo(log, "scheduledReprocess",
                        "topic=%s status=%s total=%d succeeded=%d requeued=%d parked=%d",
                        s.topic(), s.status(), s.total(), s.succeeded(), s.requeued(), s.parked());
            }
        }
    }

    /**
     * Skips the scheduled run unless explicitly enabled, so the schedule can exist in config
     * without firing until an operator turns it on.
     */
    @ApplicationScoped
    public static class SchedulerDisabled implements Scheduled.SkipPredicate {

        @ConfigProperty(name = "dlq.reprocess.schedule.enabled", defaultValue = "false")
        boolean scheduleEnabled;

        @ConfigProperty(name = "dlq.reprocess.enabled", defaultValue = "true")
        boolean reprocessEnabled;

        @Override
        public boolean test(io.quarkus.scheduler.ScheduledExecution execution) {
            return !(scheduleEnabled && reprocessEnabled);
        }
    }
}
