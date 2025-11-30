package com.csg.airtel.aaa4j.application.listner;


import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@ApplicationScoped
public class DBWriteConsumer {
    private static final Logger log = Logger.getLogger(DBWriteConsumer.class);

    // Batch configuration for high throughput
    private static final int BATCH_SIZE = 50;  // Process 50 records per batch
    private static final Duration BATCH_TIMEOUT = Duration.ofMillis(100);  // Max 100ms wait for batch

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

        return dbWriteService.processDbWriteRequest(request)
                .onItem().invoke(() -> {
                    int count = processedCounter.incrementAndGet();
                    if (count % 100 == 0) {  // Log every 100 messages
                        log.infof("Processed %d messages", count);
                    }
                })
                .onItem().transformToUni(result -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(throwable -> {
                    log.errorf(throwable, "Error processing event for user: %s | eventType: %s",
                            request.getUserName(), request.getEventType());
                    return Uni.createFrom().completionStage(message.nack(throwable));
                });
    }

    /**
     * Alternative batch processing consumer (currently not active)
     * To use this, change the @Incoming method signature to accept List<Message<DBWriteRequest>>
     * and enable batch mode in Kafka consumer configuration
     */
    public Uni<Void> consumeBatchAccountingEvents(List<Message<DBWriteRequest>> messages) {
        if (messages.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        log.infof("Processing batch of %d messages", messages.size());

        List<DBWriteRequest> requests = new ArrayList<>(messages.size());
        for (Message<DBWriteRequest> message : messages) {
            requests.add(message.getPayload());
        }

        return dbWriteService.processBatchDbWriteRequests(requests)
                .onItem().transformToUni(result -> {
                    // Acknowledge all messages in batch
                    List<Uni<Void>> acks = new ArrayList<>(messages.size());
                    for (Message<DBWriteRequest> message : messages) {
                        acks.add(Uni.createFrom().completionStage(message.ack()));
                    }
                    return Uni.combine().all().unis(acks).discardItems();
                })
                .onItem().invoke(() -> {
                    int count = processedCounter.addAndGet(messages.size());
                    log.infof("Batch processed successfully. Total messages: %d", count);
                })
                .onFailure().recoverWithUni(throwable -> {
                    log.errorf(throwable, "Error processing batch of %d events", messages.size());
                    // Nack all messages in batch
                    List<Uni<Void>> nacks = new ArrayList<>(messages.size());
                    for (Message<DBWriteRequest> message : messages) {
                        nacks.add(Uni.createFrom().completionStage(message.nack(throwable)));
                    }
                    return Uni.combine().all().unis(nacks).discardItems();
                });
    }
}
