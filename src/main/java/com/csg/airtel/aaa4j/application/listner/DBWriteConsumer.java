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
import lombok.SneakyThrows;
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

    @Inject
    @Channel("responses-out")
    MutinyEmitter<String> replyEmitter;

    @Inject
    public DBWriteConsumer(DBWriteService dbWriteService) {
        this.dbWriteService = dbWriteService;
    }

    // =========================================================================
    // DC→DR accounting channel
    // Topic: DC-DR
    // Events published by DC-side services. Both DC and DR consume this topic.
    // =========================================================================

    @Incoming("db-write-events")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeAccountingEvent(Message<DBWriteRequest> message) {
        DBWriteRequest request = message.getPayload();

        if (log.isDebugEnabled()) {
            message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .ifPresent(metadata -> log.debugf("[%s] DC-DR received from partition: %d, offset: %d, key: %s",
                            site, metadata.getPartition(), metadata.getOffset(), metadata.getKey()));
        }

        return Uni.createFrom().completionStage(message.ack())
                .onItem().transformToUni(v -> dbWriteService.processDbWriteRequest(request))
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {
                        LoggingUtil.logDebug(log, "consumeAccountingEvent", "[%s] Processed %d DC-DR messages", site, count);
                    }
                })
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeAccountingEvent", throwable,
                        "[%s] Error processing DC-DR event for user: %s | eventType: %s (already acked)",
                        site, request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem())
                .onFailure().recoverWithItem((Void) null);
    }

    // =========================================================================
    // DR→DC accounting channel (reverse direction)
    // Topic: DR-DC
    // Events published by DR-side services. Both DC and DR consume this topic.
    // =========================================================================

    @Incoming("db-write-events-reverse")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public Uni<Void> consumeReverseAccountingEvent(Message<DBWriteRequest> message) {
        DBWriteRequest request = message.getPayload();

        if (log.isDebugEnabled()) {
            message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .ifPresent(metadata -> log.debugf("[%s] DR-DC received from partition: %d, offset: %d, key: %s",
                            site, metadata.getPartition(), metadata.getOffset(), metadata.getKey()));
        }

        return Uni.createFrom().completionStage(message.ack())
                .onItem().transformToUni(v -> dbWriteService.processDbWriteRequest(request))
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {
                        LoggingUtil.logDebug(log, "consumeReverseAccountingEvent", "[%s] Processed %d DR-DC messages", site, count);
                    }
                })
                .onFailure().invoke(throwable -> LoggingUtil.logError(log, "consumeReverseAccountingEvent", throwable,
                        "[%s] Error processing DR-DC event for user: %s | eventType: %s (already acked)",
                        site, request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem())
                .onFailure().recoverWithItem((Void) null);
    }

    // =========================================================================
    // DC→DR provisioning channel (request-reply)
    // Topic: dc-provisioning
    // =========================================================================

    @Incoming("db-write-events-dr")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @SneakyThrows
    public Uni<Void> consumeAndReplyDR(Message<String> message) {
        return handleProvisioningMessage(message, "dc-provisioning");
    }

    // =========================================================================
    // DR→DC provisioning channel (reverse request-reply)
    // Topic: dr-provisioning
    // =========================================================================

    @Incoming("db-write-events-dc")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @SneakyThrows
    public Uni<Void> consumeAndReplyDC(Message<String> message) {
        return handleProvisioningMessage(message, "dr-provisioning");
    }

    // =========================================================================
    // Shared provisioning handler
    // =========================================================================

    private Uni<Void> handleProvisioningMessage(Message<String> message, String channelName) throws Exception {
        LoggingUtil.logDebug(log, "handleProvisioningMessage", "[%s] %s payload = %s", site, channelName, message.getPayload());

        ObjectMapper mapper = new ObjectMapper();
        DBWriteRequest request = mapper.readValue(message.getPayload(), DBWriteRequest.class);

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

        if (correlationHeader == null || replyTopicHeader == null) {
            LoggingUtil.logWarn(log, "handleProvisioningMessage",
                    "[%s] %s Missing reply headers — processing without reply for user: %s",
                    site, channelName, request.getUserName());
            return dbWriteService.processEvent(request)
                    .onItem().transformToUni(v -> Uni.createFrom().completionStage(message.ack()))
                    .onFailure().recoverWithUni(t -> {
                        LoggingUtil.logError(log, "handleProvisioningMessage", (Throwable) t,
                                "[%s] %s Error processing event (no reply headers) for user: %s | eventType: %s",
                                site, channelName, request.getUserName(), request.getEventType());
                        return Uni.createFrom().completionStage(message.ack());
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
                .onItem().transformToUni(v ->
                        Uni.createFrom().completionStage(message.ack())
                                .onItem().transformToUni(ignored ->
                                        sendReply(replyTopic, correlationId, "SUCCESS"))
                )
                .onFailure().recoverWithUni(throwable -> {
                    LoggingUtil.logError(log, "handleProvisioningMessage", throwable,
                            "[%s] %s Error processing event for user: %s | eventType: %s",
                            site, channelName, request.getUserName(), request.getEventType());
                    return Uni.createFrom().completionStage(message.ack())
                            .onItem().transformToUni(ignored ->
                                    sendReply(replyTopic, correlationId, "FAIL: " + throwable.getMessage())
                            );
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
