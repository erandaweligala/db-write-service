package com.csg.airtel.aaa4j.application.listner;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.*;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class DBWriteConsumer {

    private static final Logger log = Logger.getLogger(DBWriteConsumer.class);

    private final DBWriteService dbWriteService;
    private final AtomicInteger processedCounter = new AtomicInteger(0);

    @ConfigProperty(name = "app.site", defaultValue = "DC")
    String site;

    @ConfigProperty(name = "db-write.retry.max-attempts", defaultValue = "3")
    int retryMaxAttempts;

    @ConfigProperty(name = "db-write.retry.initial-backoff-ms", defaultValue = "200")
    long retryInitialBackoffMs;

    @ConfigProperty(name = "db-write.retry.max-backoff-ms", defaultValue = "2000")
    long retryMaxBackoffMs;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    @Channel("responses-out")
    MutinyEmitter<String> replyEmitter;

    @Inject
    public DBWriteConsumer(DBWriteService dbWriteService) {
        this.dbWriteService = dbWriteService;
    }

    /**
    * DC→DR accounting channel
    * Topic: DC-DR
    *Events published by DC-side services. Both DC and DR consume this topic.
     */
    @Incoming("db-write-events")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAccountingEvent(Message<DBWriteRequest> message) {
        DBWriteRequest request = message.getPayload();

        if (log.isDebugEnabled()) {
            message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .ifPresent(metadata -> log.debugf("[%s] DC-DR received from partition: %d, offset: %d, key: %s",
                            site, metadata.getPartition(), metadata.getOffset(), metadata.getKey()));
        }

        return dbWriteService.processDbWriteRequest(request)
                .onFailure().invoke(throwable -> LoggingUtil.logWarn(log, "consumeAccountingEvent",
                        "[%s] DC-DR DB write failed, will retry (user=%s, eventType=%s): %s",
                        site, request.getUserName(), request.getEventType(), throwable.getMessage()))
                .onFailure().retry()
                    .withBackOff(Duration.ofMillis(retryInitialBackoffMs), Duration.ofMillis(retryMaxBackoffMs))
                    .atMost(retryMaxAttempts)
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {
                        LoggingUtil.logDebug(log, "consumeAccountingEvent", "[%s] Processed %d DC-DR messages", site, count);
                    }
                })
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeAccountingEvent", throwable,
                        "[%s] DC-DR retries exhausted, routing to DLQ for user: %s | eventType: %s",
                        site, request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem());
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

        if (log.isDebugEnabled()) {
            message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .ifPresent(metadata -> log.debugf("[%s] DR-DC received from partition: %d, offset: %d, key: %s",
                            site, metadata.getPartition(), metadata.getOffset(), metadata.getKey()));
        }

        return dbWriteService.processDbWriteRequest(request)
                .onFailure().invoke(throwable -> LoggingUtil.logWarn(log, "consumeReverseAccountingEvent",
                        "[%s] DR-DC DB write failed, will retry (user=%s, eventType=%s): %s",
                        site, request.getUserName(), request.getEventType(), throwable.getMessage()))
                .onFailure().retry()
                    .withBackOff(Duration.ofMillis(retryInitialBackoffMs), Duration.ofMillis(retryMaxBackoffMs))
                    .atMost(retryMaxAttempts)
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {
                        LoggingUtil.logDebug(log, "consumeReverseAccountingEvent", "[%s] Processed %d DR-DC messages", site, count);
                    }
                })
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeReverseAccountingEvent", throwable,
                        "[%s] DR-DC retries exhausted, routing to DLQ for user: %s | eventType: %s",
                        site, request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem());
    }


    @Incoming("db-write-events-dr")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAndReplyDR(Message<String> message) {
        return handleProvisioningMessage(message, "dc-provisioning");
    }


    @Incoming("db-write-events-dc")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAndReplyDC(Message<String> message) {
        return handleProvisioningMessage(message, "dr-provisioning");
    }


    private Uni<Void> handleProvisioningMessage(Message<String> message, String channelName) {
        LoggingUtil.logDebug(log, "handleProvisioningMessage", "[%s] %s payload = %s", site, channelName, message.getPayload());

        DBWriteRequest request;
        try {
            request = objectMapper.readValue(message.getPayload(), DBWriteRequest.class);
        } catch (Exception e) {
            LoggingUtil.logError(log, "handleProvisioningMessage", e,
                    "[%s] %s Failed to deserialize payload", site, channelName);
            return Uni.createFrom().voidItem();
        }

        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).get();

        if (log.isDebugEnabled()) {
            metadata.getHeaders().forEach(h ->
                    LoggingUtil.logDebug(log, "handleProvisioningMessage", "[%s] %s Header: %s = %s", site, channelName,
                            h.key(), new String(h.value(), StandardCharsets.UTF_8))
            );
        }

        var correlationHeader = metadata.getHeaders().lastHeader("kafka_correlationId");
        var replyTopicHeader  = metadata.getHeaders().lastHeader("kafka_replyTopic");

        DBWriteRequest finalRequest = request;
        Uni<Void> processedWithRetry = dbWriteService.processEvent(request)
                .onFailure().invoke(throwable -> LoggingUtil.logWarn(log, "handleProvisioningMessage",
                        "[%s] %s DB write failed, will retry (user=%s, eventType=%s): %s",
                        site, channelName, finalRequest.getUserName(), finalRequest.getEventType(),
                        throwable.getMessage()))
                .onFailure().retry()
                    .withBackOff(Duration.ofMillis(retryInitialBackoffMs), Duration.ofMillis(retryMaxBackoffMs))
                    .atMost(retryMaxAttempts);

        if (correlationHeader == null || replyTopicHeader == null) {
            LoggingUtil.logWarn(log, "handleProvisioningMessage",
                    "[%s] %s Missing reply headers — processing without reply for user: %s",
                    site, channelName, request.getUserName());
            return processedWithRetry
                    .onFailure().invoke(t -> LoggingUtil.logError(log, "handleProvisioningMessage", t,
                            "[%s] %s Retries exhausted (no reply headers), routing to DLQ for user: %s | eventType: %s",
                            site, channelName, finalRequest.getUserName(), finalRequest.getEventType()));
        }

        byte[] correlationId = correlationHeader.value();
        String replyTopic    = new String(replyTopicHeader.value(), StandardCharsets.UTF_8);
        LoggingUtil.logDebug(log, "handleProvisioningMessage", "[%s] %s Reply topic: %s", site, channelName, replyTopic);

        return processedWithRetry
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) LoggingUtil.logDebug(log, "handleProvisioningMessage",
                            "[%s] Processed %d %s messages", site, count, channelName);
                })
                .onItem().transformToUni(v -> sendReply(replyTopic, correlationId, "SUCCESS"))
                .onFailure().recoverWithUni(throwable -> {
                    LoggingUtil.logError(log, "handleProvisioningMessage", throwable,
                            "[%s] %s Retries exhausted, replying FAIL and routing to DLQ for user: %s | eventType: %s",
                            site, channelName, finalRequest.getUserName(), finalRequest.getEventType());
                    return sendReply(replyTopic, correlationId, "FAIL: " + throwable.getMessage())
                            .onItem().transformToUni(v -> Uni.createFrom().<Void>failure(throwable));
                });
    }

    private Uni<Void> sendReply(String replyTopic, byte[] correlationId, String payload) {
        OutgoingKafkaRecordMetadata<Object> meta = OutgoingKafkaRecordMetadata.builder()
                .withTopic(replyTopic)
                .withHeaders(new RecordHeaders().add("kafka_correlationId", correlationId))
                .build();

        Message<String> replyMessage = Message.of(payload).addMetadata(meta);
        return replyEmitter.sendMessage(replyMessage);
    }
}
