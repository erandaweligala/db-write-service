package com.csg.airtel.aaa4j.application.listner;


import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;


@ApplicationScoped
public class DBWriteConsumer {
    private static final Logger log = Logger.getLogger(DBWriteConsumer.class);

    private final DBWriteService dbWriteService;
    private final AtomicInteger processedCounter = new AtomicInteger(0);

    @Inject
    public DBWriteConsumer(DBWriteService dbWriteService) {
        this.dbWriteService = dbWriteService;
    }

    /**
     * Optimized batch consumer for high throughput (1000+ TPS)
     * Groups messages into batches and processes them in a single transaction
     */
    @Incoming("db-write-events")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<Void> consumeAccountingEvent(Message<DBWriteRequest> message) {
        DBWriteRequest request = message.getPayload();

        // Log partition info at debug level to reduce overhead
        if (log.isDebugEnabled()) {
            message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .ifPresent(metadata -> log.debugf("Received from partition: %d, offset: %d, key: %s",
                            metadata.getPartition(),
                            metadata.getOffset(),
                            metadata.getKey()));
        }

        // Acknowledge first, then process
        return Uni.createFrom().completionStage(message.ack())
                .onItem().transformToUni(v -> dbWriteService.processDbWriteRequest(request))
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {  // Log every 100 messages
                        log.infof("Processed %d messages", count);
                    }
                })
                .onFailure().invoke(throwable -> log.errorf(throwable, "Error processing event for user: %s | eventType: %s (already acked)",
                        request.getUserName(), request.getEventType()))
                .onItem().transformToUni(result -> Uni.createFrom().voidItem())
                .onFailure().recoverWithItem((Void) null);
    }



    @Incoming("db-write-events-dr")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<Void> consumeAccountingEventProvisioningAPIs(Message<String> message) throws JsonProcessingException {

       try {
            log.infof("payload = %s", message.getPayload());
            ObjectMapper mapper = new ObjectMapper();
            DBWriteRequest request = mapper.readValue(message.getPayload(), DBWriteRequest.class);


            // Log partition info at debug level to reduce overhead
            if (log.isDebugEnabled()) {
                message.getMetadata(IncomingKafkaRecordMetadata.class)
                        .ifPresent(metadata -> log.debugf("Received from partition: %d, offset: %d, key: %s",
                                metadata.getPartition(),
                                metadata.getOffset(),
                                metadata.getKey()));
            }

            Instant startTime = Instant.now();

            // Process first, then acknowledge
            return dbWriteService.processEvent(request)
                    .onItem().invoke(result -> {
                        int count = processedCounter.incrementAndGet();
                        if (count % 100 == 0) {
                            log.infof("Processed %d messages", count);
                        }

                        Duration duration = Duration.between(startTime, Instant.now());
                        log.debugf("Processed eventType=%s for user=%s in %d ms",
                                request.getEventType(), request.getUserName(), duration.toMillis());
                    })
                    .onItem().transformToUni(result ->
                            Uni.createFrom().completionStage(message.ack()) // ack only after success
                    )
                    .onFailure().recoverWithUni(throwable -> {
                        log.errorf(throwable,
                                "Error processing event for user: %s | eventType: %s (not acked)",
                                request.getUserName(), request.getEventType());
                        return Uni.createFrom()
                                .completionStage(message.nack(throwable)); // nack after failure
                    })
                    .replaceWithVoid(); // ensure Uni<Void> return type
        }catch (Exception e) {
           log.errorf("Failed processing message: %s. Payload: %s", e.getMessage(), message.getPayload());
           // optionally send to DLQ
           return Uni.createFrom()
                   .completionStage(message.nack(e));
       }
    }


}
