package com.csg.airtel.aaa4j.application.listner;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.application.common.TraceIdGenerator;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequestUMS;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.domain.service.UMSDbWriteService;
import com.csg.airtel.aaa4j.infrastructure.DlqMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka consumer for MySQL write events originating from UMS.
 *
 * <h3>Two consumers, two topics — fire-and-forget</h3>
 *
 * <pre>
 * DC active:
 *   UMS (DC) ──publishes──► ums-mysql-dc (DC Kafka)
 *                               │
 *                  ┌────────────┴────────────┐
 *                  │  mirrored to DR Kafka   │
 *                  ▼                         ▼
 *         [DC consumer]              [DR consumer]
 *       writes DC MySQL            writes DR MySQL
 *       (no reply needed)          (no reply needed)
 * </pre>
 *
 * <p>Both sites write silently. No reply is sent back to UMS.
 */
@ApplicationScoped
public class UMSWriteConsumer {

    private static final Logger log = Logger.getLogger(UMSWriteConsumer.class);
    private static final String MDC_TRACE_ID  = "traceId";
    private static final String MDC_USER_NAME = "userName";
    private static final String HEADER_TRACE_ID = "traceId";

    // Channel identifiers used as the dropped-event metric's "channel" tag.
    // These channels use failure-strategy: ignore, so terminal failures are
    // acknowledged and discarded (payload lost) rather than dead-lettered.
    private static final String CH_UMS_DC = "ums-mysql-dc";
    private static final String CH_UMS_DR = "ums-mysql-dr";

    private final UMSDbWriteService mysqlWriteService;
    private final ExceptionMetricsService exceptionMetrics;
    private final DlqMetrics dlqMetrics;
    private final AtomicInteger processedCounter = new AtomicInteger(0);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    public UMSWriteConsumer(UMSDbWriteService mysqlWriteService,
                            ExceptionMetricsService exceptionMetrics,
                            DlqMetrics dlqMetrics) {
        this.mysqlWriteService = mysqlWriteService;
        this.exceptionMetrics = exceptionMetrics;
        this.dlqMetrics = dlqMetrics;
    }

    // =========================================================================
    // DC topic consumer
    // =========================================================================

    @Incoming("mysql-db-write-events")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeDC(Message<String> message) {
        return handleMessage(message, "DR", CH_UMS_DC);
    }

    // =========================================================================
    // DR topic consumer
    // =========================================================================

    @Incoming("mysql-db-write-events-mirrored")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeDR(Message<String> message) {
        return handleMessage(message, "DC", CH_UMS_DR);
    }

    // =========================================================================
    // Shared handler — write and acknowledge, no reply
    // =========================================================================

    private Uni<Void> handleMessage(Message<String> message, String label, String channel) {

        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);

        String headerTraceId = extractTraceHeader(metadata);

        DBWriteRequestUMS request;
        try {
            request = objectMapper.readValue(message.getPayload(), DBWriteRequestUMS.class);
        } catch (Exception e) {
            String traceId = headerTraceId != null ? headerTraceId : TraceIdGenerator.generateTraceId();
            bindMdc(traceId, null);
            try {
                exceptionMetrics.recordException(e,
                        ExceptionMetricsService.Layer.CONSUMER,
                        ExceptionMetricsService.Source.KAFKA);
                // failure-strategy: ignore — this poison message is discarded, not dead-lettered.
                dlqMetrics.recordDroppedEvent(channel, DlqMetrics.Reason.DESERIALIZATION);
                LoggingUtil.logError(log, "handleMessage", e,
                        "[MySQL][%s] Failed to deserialize payload — discarding (failure-strategy: ignore)", label);
            } finally {
                clearMdc();
            }
            return Uni.createFrom().failure(e);
        }

        String traceId = headerTraceId != null ? headerTraceId : TraceIdGenerator.generateTraceId();
        request.setTraceId(traceId);
        bindMdc(traceId, request);

        return mysqlWriteService.processEvent(request)
                .onItem().invoke(() -> incrementAndLog(label))
                .onFailure().invoke(t -> {
                    exceptionMetrics.recordException(t,
                            ExceptionMetricsService.Layer.CONSUMER,
                            ExceptionMetricsService.Source.KAFKA);
                    // Swallowed below via recoverWithItem — the payload is lost, so count it.
                    dlqMetrics.recordDroppedEvent(channel, t);
                    LoggingUtil.logError(log, "handleMessage", t,
                            "[MySQL][%s] Write failed for user: %s | eventType: %s | table: %s",
                            label, request.getUserName(), request.getEventType(),
                            request.getTableName());
                })
                // Swallow failure — log it, acknowledge the message, avoid partition stall.
                // A DLT will capture terminal failures via the failure-strategy config.
                .onFailure().recoverWithItem((Void) null);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void incrementAndLog(String label) {
        int count = processedCounter.incrementAndGet();
        if (count % 100 == 0) {
            LoggingUtil.logDebug(log, "incrementAndLog",
                    "[MySQL][%s] Processed %d messages", label, count);
        }
    }

    private String extractTraceHeader(IncomingKafkaRecordMetadata<?, ?> metadata) {
        if (metadata == null) return null;
        Header h = metadata.getHeaders().lastHeader(HEADER_TRACE_ID);
        if (h == null || h.value() == null) return null;
        return new String(h.value(), StandardCharsets.UTF_8);
    }

    private static void bindMdc(String traceId, DBWriteRequestUMS request) {
        if (traceId != null) MDC.put(MDC_TRACE_ID, traceId);
        if (request != null && request.getUserName() != null)
            MDC.put(MDC_USER_NAME, request.getUserName());
    }

    private static void clearMdc() {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_USER_NAME);
    }
}