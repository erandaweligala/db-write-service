package com.csg.airtel.aaa4j.application.listner;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.application.common.TraceIdGenerator;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.infrastructure.KafkaErrorMetrics;
import com.csg.airtel.aaa4j.infrastructure.KafkaFailureClassifier;
import com.csg.airtel.aaa4j.infrastructure.ResilientDbWriteExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.*;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class DBWriteConsumer {

    private static final Logger log = Logger.getLogger(DBWriteConsumer.class);

    private static final String MDC_TRACE_ID = "traceId";
    private static final String MDC_USER_NAME = "userName";
    private static final String HEADER_TRACE_ID = "traceId";

    private final ResilientDbWriteExecutor executor;
    private final KafkaErrorMetrics errorMetrics;
    private final AtomicInteger processedCounter = new AtomicInteger(0);

    @ConfigProperty(name = "app.site", defaultValue = "DC")
    String site;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    @Channel("responses-out")
    MutinyEmitter<String> replyEmitter;

    @Inject
    public DBWriteConsumer(ResilientDbWriteExecutor executor, KafkaErrorMetrics errorMetrics) {
        this.executor = executor;
        this.errorMetrics = errorMetrics;
    }

    /**
     * DC→DR accounting channel
     * Topic: DC-DR
     * Events published by DC-side services. Both DC and DR consume this topic.
     */
    @Incoming("db-write-events")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAccountingEvent(Message<DBWriteRequest> message) {
        DBWriteRequest request = message.getPayload();
        IncomingKafkaRecordMetadata<?, ?> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        String traceId = resolveTraceId(metadata, request);
        bindMdc(traceId, request);

        if (metadata != null && log.isDebugEnabled()) {
            log.debugf("[%s] DC-DR received from partition: %d, offset: %d, key: %s",
                    site, metadata.getPartition(), metadata.getOffset(), metadata.getKey());
        }
        LoggingUtil.logInfo(log, "consumeAccountingEvent",
                "[%s] consume eventType=%s table=%s user=%s",
                site, request.getEventType(), request.getTableName(), request.getUserName());

        return executor.execute("db-write-events", request)
                .onItem().invoke(() -> incrementAndMaybeLog("consumeAccountingEvent", "DC-DR"))
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeAccountingEvent", throwable,
                        "[%s] DC-DR routing to DLT after retries: user=%s | eventType=%s | transient=%s",
                        site, request.getUserName(), request.getEventType(),
                        KafkaFailureClassifier.isTransient(throwable)))
                .eventually((Runnable) DBWriteConsumer::clearMdc);
    }

    /**
     * DR→DC accounting channel (reverse direction)
     * Topic: DR-DC
     * Events published by DR-side services. Both DC and DR consume this topic.
     */
    @Incoming("db-write-events-reverse")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeReverseAccountingEvent(Message<DBWriteRequest> message) {
        DBWriteRequest request = message.getPayload();
        IncomingKafkaRecordMetadata<?, ?> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        String traceId = resolveTraceId(metadata, request);
        bindMdc(traceId, request);

        if (metadata != null && log.isDebugEnabled()) {
            log.debugf("[%s] DR-DC received from partition: %d, offset: %d, key: %s",
                    site, metadata.getPartition(), metadata.getOffset(), metadata.getKey());
        }
        LoggingUtil.logInfo(log, "consumeReverseAccountingEvent",
                "[%s] consume eventType=%s table=%s user=%s",
                site, request.getEventType(), request.getTableName(), request.getUserName());

        return executor.execute("db-write-events-reverse", request)
                .onItem().invoke(() -> incrementAndMaybeLog("consumeReverseAccountingEvent", "DR-DC"))
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeReverseAccountingEvent", throwable,
                        "[%s] DR-DC routing to DLT after retries: user=%s | eventType=%s | transient=%s",
                        site, request.getUserName(), request.getEventType(),
                        KafkaFailureClassifier.isTransient(throwable)))
                .eventually((Runnable) DBWriteConsumer::clearMdc);
    }


    @Incoming("db-write-events-scheduler")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeScheduler(Message<String> message) {
        return handleStringPayload(message, "db-write-events-scheduler", "scheduler", "consumeScheduler");
    }


    @Incoming("db-write-events-scheduler-dr")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeSchedulerDr(Message<String> message) {
        return handleStringPayload(message, "db-write-events-scheduler-dr", "scheduler-dr", "consumeSchedulerDr");
    }


    @Incoming("db-write-events-dr")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAndReplyDR(Message<String> message) {
        return handleProvisioningMessage(message, "db-write-events-dr", "dc-provisioning");
    }


    @Incoming("db-write-events-dc")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAndReplyDC(Message<String> message) {
        return handleProvisioningMessage(message, "db-write-events-dc", "dr-provisioning");
    }


    private Uni<Void> handleStringPayload(Message<String> message, String channel, String label, String method) {
        IncomingKafkaRecordMetadata<?, ?> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        String headerTraceId = extractTraceHeader(metadata);

        DBWriteRequest request;
        try {
            request = objectMapper.readValue(message.getPayload(), DBWriteRequest.class);
        } catch (Exception e) {
            // Deserialization is a permanent failure: propagate so Smallrye routes the
            // raw bytes to the DLT instead of silently dropping them.
            errorMetrics.recordDeserializationFailure(channel);
            String traceId = headerTraceId != null ? headerTraceId : TraceIdGenerator.generateTraceId();
            bindMdc(traceId, null);
            try {
                LoggingUtil.logError(log, method, e,
                        "[%s] %s failed to deserialize payload — routing to DLT", site, label);
            } finally {
                clearMdc();
            }
            return Uni.createFrom().failure(e);
        }

        String traceId = headerTraceId != null ? headerTraceId : TraceIdGenerator.generateTraceId();
        request.setTraceId(traceId);
        bindMdc(traceId, request);

        if (metadata != null && log.isDebugEnabled()) {
            log.debugf("[%s] %s received from partition: %d, offset: %d, key: %s",
                    site, label, metadata.getPartition(), metadata.getOffset(), metadata.getKey());
        }
        LoggingUtil.logInfo(log, method,
                "[%s] consume eventType=%s table=%s user=%s",
                site, request.getEventType(), request.getTableName(), request.getUserName());

        DBWriteRequest finalRequest = request;
        return executor.execute(channel, request)
                .onItem().invoke(() -> incrementAndMaybeLog(method, label))
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, method, throwable,
                        "[%s] %s routing to DLT after retries: user=%s | eventType=%s | transient=%s",
                        site, label, finalRequest.getUserName(), finalRequest.getEventType(),
                        KafkaFailureClassifier.isTransient(throwable)))
                .eventually((Runnable) DBWriteConsumer::clearMdc);
    }


    private Uni<Void> handleProvisioningMessage(Message<String> message, String channel, String channelName) {
        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
        String headerTraceId = extractTraceHeader(metadata);

        DBWriteRequest request;
        try {
            request = objectMapper.readValue(message.getPayload(), DBWriteRequest.class);
        } catch (Exception e) {
            errorMetrics.recordDeserializationFailure(channel);
            String traceId = headerTraceId != null ? headerTraceId : TraceIdGenerator.generateTraceId();
            bindMdc(traceId, null);
            try {
                LoggingUtil.logError(log, "handleProvisioningMessage", e,
                        "[%s] %s failed to deserialize payload — routing to DLT", site, channelName);
            } finally {
                clearMdc();
            }
            return Uni.createFrom().failure(e);
        }

        String traceId = headerTraceId != null ? headerTraceId : TraceIdGenerator.generateTraceId();
        request.setTraceId(traceId);
        bindMdc(traceId, request);

        if (log.isDebugEnabled()) {
            LoggingUtil.logDebug(log, "handleProvisioningMessage",
                    "[%s] %s payload received user=%s eventType=%s",
                    site, channelName, request.getUserName(), request.getEventType());
            if (metadata != null) {
                metadata.getHeaders().forEach(h ->
                        LoggingUtil.logDebug(log, "handleProvisioningMessage", "[%s] %s Header: %s = %s",
                                site, channelName, h.key(), new String(h.value(), StandardCharsets.UTF_8)));
            }
        }

        Header correlationHeader = metadata != null ? metadata.getHeaders().lastHeader("kafka_correlationId") : null;
        Header replyTopicHeader  = metadata != null ? metadata.getHeaders().lastHeader("kafka_replyTopic") : null;

        DBWriteRequest finalRequest = request;

        if (correlationHeader == null || replyTopicHeader == null) {
            LoggingUtil.logWarn(log, "handleProvisioningMessage",
                    "[%s] %s Missing reply headers — processing without reply for user: %s",
                    site, channelName, request.getUserName());
            return executor.execute(channel, request)
                    .onFailure().invoke(throwable -> LoggingUtil.logError(log, "handleProvisioningMessage", throwable,
                            "[%s] %s routing to DLT (no reply headers) user=%s | eventType=%s | transient=%s",
                            site, channelName, finalRequest.getUserName(), finalRequest.getEventType(),
                            KafkaFailureClassifier.isTransient(throwable)))
                    .eventually((Runnable) DBWriteConsumer::clearMdc);
        }

        byte[] correlationId = correlationHeader.value();
        String replyTopic    = new String(replyTopicHeader.value(), StandardCharsets.UTF_8);
        LoggingUtil.logDebug(log, "handleProvisioningMessage",
                "[%s] %s Reply topic: %s", site, channelName, replyTopic);

        // Run the DB write through the resilience executor; only AFTER retries are exhausted
        // do we either reply FAIL to the caller and route the original record to the DLT.
        return executor.execute(channel, request)
                .onItem().invoke(() -> incrementAndMaybeLog("handleProvisioningMessage", channelName))
                .onItem().transformToUni(v -> sendReply(replyTopic, correlationId, "SUCCESS"))
                .onFailure().call(throwable -> {
                    LoggingUtil.logError(log, "handleProvisioningMessage", throwable,
                            "[%s] %s routing to DLT after retries: user=%s | eventType=%s | transient=%s",
                            site, channelName, finalRequest.getUserName(), finalRequest.getEventType(),
                            KafkaFailureClassifier.isTransient(throwable));
                    return sendReply(replyTopic, correlationId, "FAIL: " + throwable.getMessage())
                            .onFailure().recoverWithItem((Void) null);
                })
                .eventually((Runnable) DBWriteConsumer::clearMdc);
    }

    private Uni<Void> sendReply(String replyTopic, byte[] correlationId, String payload) {
        OutgoingKafkaRecordMetadata<Object> meta = OutgoingKafkaRecordMetadata.builder()
                .withTopic(replyTopic)
                .withHeaders(new RecordHeaders().add("kafka_correlationId", correlationId))
                .build();

        Message<String> replyMessage = Message.of(payload).addMetadata(meta);
        return replyEmitter.sendMessage(replyMessage);
    }

    private void incrementAndMaybeLog(String method, String label) {
        int count = processedCounter.incrementAndGet();
        if (count % 100 == 0 && log.isInfoEnabled()) {
            LoggingUtil.logInfo(log, method, "[%s] Processed %d %s messages",
                    site, count, label);
        }
    }

    private String resolveTraceId(IncomingKafkaRecordMetadata<?, ?> metadata, DBWriteRequest request) {
        String traceId = extractTraceHeader(metadata);
        if (traceId == null) {
            traceId = TraceIdGenerator.generateTraceId();
        }
        if (request != null) {
            request.setTraceId(traceId);
        }
        return traceId;
    }

    private String extractTraceHeader(IncomingKafkaRecordMetadata<?, ?> metadata) {
        if (metadata == null) return null;
        Header h = metadata.getHeaders().lastHeader(HEADER_TRACE_ID);
        if (h == null || h.value() == null) return null;
        return new String(h.value(), StandardCharsets.UTF_8);
    }

    private static void bindMdc(String traceId, DBWriteRequest request) {
        if (traceId != null) {
            MDC.put(MDC_TRACE_ID, traceId);
        }
        if (request != null && request.getUserName() != null) {
            MDC.put(MDC_USER_NAME, request.getUserName());
        }
    }

    private static void clearMdc() {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_USER_NAME);
    }
}
