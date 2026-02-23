package com.csg.airtel.aaa4j.application.listner;

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
import org.eclipse.microprofile.reactive.messaging.*;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class DBWriteConsumer {

    private static final Logger log = Logger.getLogger(DBWriteConsumer.class);

    private final DBWriteService dbWriteService;
    private final AtomicInteger processedCounter = new AtomicInteger(0);

    @Inject
    @Channel("responses-out")
    MutinyEmitter<String> replyEmitter;

    @Inject
    public DBWriteConsumer(DBWriteService dbWriteService) {
        this.dbWriteService = dbWriteService;
    }

    /**
     * Consumer for the DC→DR accounting channel.
     * Acknowledges first (at-most-once on failure) then processes.
     */
    @Incoming("db-write-events")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<Void> consumeAccountingEvent(Message<DBWriteRequest> message) {
        DBWriteRequest request = message.getPayload();

        if (log.isDebugEnabled()) {
            message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .ifPresent(metadata -> log.debugf("Received from partition: %d, offset: %d, key: %s",
                            metadata.getPartition(),
                            metadata.getOffset(),
                            metadata.getKey()));
        }

        return Uni.createFrom().completionStage(message.ack())
                .onItem().transformToUni(v -> dbWriteService.processDbWriteRequest(request))
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {
                        log.infof("Processed %d messages", count);
                    }
                })
                .onFailure().invoke(throwable -> log.errorf(throwable,
                        "Error processing event for user: %s | eventType: %s (already acked)",
                        request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem())
                .onFailure().recoverWithItem((Void) null);
    }

    /**
     * Consumer for the DR provisioning channel (request-reply with Spring).
     *
     * Fix summary:
     *  - On SUCCESS  → ack() then send "SUCCESS" reply.
     *  - On FAILURE  → ack() (not nack) then send "FAIL:..." reply.
     *
     * Previously nack() was called on failure, which with failure-strategy=fail
     * halted the entire consumer. Because inserts are now idempotent in
     * DBWriteRepository (duplicate unique-constraint violations are silently
     * skipped), a genuine fatal error is extremely rare. We ack unconditionally
     * so the consumer keeps running, and communicate the outcome via the reply.
     */
    @Incoming("db-write-events-dr")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @SneakyThrows
    public Uni<Void> consumeAndReply(Message<String> message) {

        log.infof("payload = %s", message.getPayload());

        ObjectMapper mapper = new ObjectMapper();
        DBWriteRequest request = mapper.readValue(message.getPayload(), DBWriteRequest.class);

        IncomingKafkaRecordMetadata<?, ?> metadata =
                message.getMetadata(IncomingKafkaRecordMetadata.class).get();

        metadata.getHeaders().forEach(h ->
                log.infof("Header: %s = %s", h.key(), new String(h.value(), StandardCharsets.UTF_8))
        );

        var correlationHeader = metadata.getHeaders().lastHeader("kafka_correlationId");
        var replyTopicHeader  = metadata.getHeaders().lastHeader("kafka_replyTopic");

        if (correlationHeader == null || replyTopicHeader == null) {
            log.warnf("Missing reply headers — processing without reply for user: %s", request.getUserName());
            return dbWriteService.processEvent(request)
                    .onItem().transformToUni(v -> Uni.createFrom().completionStage(message.ack()))
                    .onFailure().recoverWithUni(t -> {
                        log.errorf(t, "Error processing event (no reply headers) for user: %s | eventType: %s",
                                request.getUserName(), request.getEventType());
                        // Ack even on failure — consumer must keep running.
                        // The duplicate-safe insert means this is a truly unexpected
                        // error; log it and move on.
                        return Uni.createFrom().completionStage(message.ack());
                    });
        }

        byte[] correlationId = correlationHeader.value();
        String replyTopic    = new String(replyTopicHeader.value(), StandardCharsets.UTF_8);
        log.infof("Reply topic: %s", replyTopic);

        return dbWriteService.processEvent(request)
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) log.infof("Processed %d DR messages", count);
                })
                .onItem().transformToUni(v ->
                        // SUCCESS path: ack then reply
                        Uni.createFrom().completionStage(message.ack())
                                .onItem().transformToUni(ignored ->
                                        sendReply(replyTopic, correlationId, "SUCCESS"))
                )
                .onFailure().recoverWithUni(throwable -> {
                    log.errorf(throwable, "Error processing DR event for user: %s | eventType: %s",
                            request.getUserName(), request.getEventType());
                    // FAILURE path: ack (not nack) so the consumer keeps running,
                    // then send a FAIL reply so the caller is still notified.
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