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
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class DBWriteConsumer {

    private static final Logger log = Logger.getLogger(DBWriteConsumer.class);

    private final DBWriteService dbWriteService;
    private final AtomicInteger processedCounter = new AtomicInteger(0);

    @ConfigProperty(name = "app.site", defaultValue = "DC")
    String site;

    @ConfigProperty(name = "mp.messaging.incoming.db-write-events.topic", defaultValue = "DC-DR")
    String accountingTopic;

    @ConfigProperty(name = "mp.messaging.incoming.db-write-events-reverse.topic", defaultValue = "DR-DC")
    String accountingReverseTopic;

    @ConfigProperty(name = "mp.messaging.incoming.db-write-events-dr.topic", defaultValue = "dc-provisioning")
    String provisioningDcTopic;

    @ConfigProperty(name = "mp.messaging.incoming.db-write-events-dc.topic", defaultValue = "dr-provisioning")
    String provisioningDrTopic;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    KafkaRetryHandler retryHandler;

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
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {
                        LoggingUtil.logDebug(log, "consumeAccountingEvent", "[%s] Processed %d DC-DR messages", site, count);
                    }
                })
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeAccountingEvent", throwable,
                        "[%s] Error processing DC-DR event for user: %s | eventType: %s — requeueing",
                        site, request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem())
                .onFailure().recoverWithUni(throwable ->
                        retryHandler.requeueOnFailure(message, accountingTopic, request, throwable));
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
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {
                        LoggingUtil.logDebug(log, "consumeReverseAccountingEvent", "[%s] Processed %d DR-DC messages", site, count);
                    }
                })
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeReverseAccountingEvent", throwable,
                        "[%s] Error processing DR-DC event for user: %s | eventType: %s — requeueing",
                        site, request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem())
                .onFailure().recoverWithUni(throwable ->
                        retryHandler.requeueOnFailure(message, accountingReverseTopic, request, throwable));
    }


    @Incoming("db-write-events-dr")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAndReplyDR(Message<String> message) {
        return handleProvisioningMessage(message, "dc-provisioning", provisioningDcTopic);
    }


    @Incoming("db-write-events-dc")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAndReplyDC(Message<String> message) {
        return handleProvisioningMessage(message, "dr-provisioning", provisioningDrTopic);
    }


    private Uni<Void> handleProvisioningMessage(Message<String> message, String channelName, String sourceTopic) {
        LoggingUtil.logDebug(log, "handleProvisioningMessage", "[%s] %s payload = %s", site, channelName, message.getPayload());

        DBWriteRequest request;
        try {
            request = objectMapper.readValue(message.getPayload(), DBWriteRequest.class);
        } catch (Exception e) {
            // Deserialization failure is not retriable — payload is malformed.
            LoggingUtil.logError(log, "handleProvisioningMessage", e,
                    "[%s] %s Failed to deserialize payload — dropping (non-retriable)", site, channelName);
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

        if (correlationHeader == null || replyTopicHeader == null) {
            LoggingUtil.logWarn(log, "handleProvisioningMessage",
                    "[%s] %s Missing reply headers — processing without reply for user: %s",
                    site, channelName, request.getUserName());
            return dbWriteService.processEvent(request)
                    .onFailure().recoverWithUni(t -> {
                        LoggingUtil.logError(log, "handleProvisioningMessage", (Throwable) t,
                                "[%s] %s Error processing event (no reply headers) for user: %s | eventType: %s — requeueing",
                                site, channelName, finalRequest.getUserName(), finalRequest.getEventType());
                        return retryHandler.requeueOnFailure(message, sourceTopic, message.getPayload(), (Throwable) t);
                    });
        }

        byte[] correlationId = correlationHeader.value();
        String replyTopic    = new String(replyTopicHeader.value(), StandardCharsets.UTF_8);
        LoggingUtil.logDebug(log, "handleProvisioningMessage", "[%s] %s Reply topic: %s", site, channelName, replyTopic);

        return dbWriteService.processEvent(request)
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) LoggingUtil.logDebug(log, "handleProvisioningMessage",
                            "[%s] Processed %d %s messages", site, count, channelName);
                })
                .onItem().transformToUni(v -> sendReply(replyTopic, correlationId, "SUCCESS"))
                .onFailure().recoverWithUni(throwable -> {
                    LoggingUtil.logError(log, "handleProvisioningMessage", throwable,
                            "[%s] %s Error processing event for user: %s | eventType: %s — requeueing and notifying caller",
                            site, channelName, finalRequest.getUserName(), finalRequest.getEventType());
                    return retryHandler.requeueOnFailure(message, sourceTopic, message.getPayload(), throwable)
                            .onItem().transformToUni(v -> sendReply(replyTopic, correlationId, "FAIL: " + throwable.getMessage()));
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
