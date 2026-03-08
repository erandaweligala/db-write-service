package com.csg.airtel.aaa4j.application.listner;

import com.csg.airtel.aaa4j.application.config.SiteConfig;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.model.SyncEventEnvelope;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import com.csg.airtel.aaa4j.infrastructure.EventDeduplicationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes sync events from the remote site's Kafka topic and applies
 * them to the local database.
 *
 * DC site consumes from "sync-inbound" channel → mapped to DR-DC topic
 * DR site consumes from "sync-inbound" channel → mapped to DC-DR topic
 *
 * Key safety mechanisms:
 *   1. Source site check: skip events that originated from this site
 *   2. EventId dedup: skip events already processed (prevents loops)
 *   3. Mark eventId after processing: so SyncEventProducer won't re-publish
 */
@ApplicationScoped
public class SyncEventConsumer {

    private static final Logger log = Logger.getLogger(SyncEventConsumer.class);

    private final DBWriteService dbWriteService;
    private final EventDeduplicationService dedupService;
    private final SiteConfig siteConfig;
    private final ObjectMapper objectMapper;
    private final AtomicLong syncedCount = new AtomicLong(0);

    @Inject
    public SyncEventConsumer(DBWriteService dbWriteService,
                             EventDeduplicationService dedupService,
                             SiteConfig siteConfig,
                             ObjectMapper objectMapper) {
        this.dbWriteService = dbWriteService;
        this.dedupService = dedupService;
        this.siteConfig = siteConfig;
        this.objectMapper = objectMapper;
    }

    /**
     * Consume a sync event from the remote site.
     * Ack-first strategy (at-most-once) to prevent consumer halt.
     * Idempotent inserts in DBWriteRepository handle any reprocessing.
     */
    @Incoming("sync-inbound")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<Void> consumeSyncEvent(Message<String> message) {
        String rawPayload = message.getPayload();

        return Uni.createFrom().completionStage(message.ack())
                .onItem().transformToUni(v -> processSyncMessage(rawPayload))
                .onFailure().invoke(t ->
                        log.errorf(t, "Error processing sync event (already acked): %s",
                                truncate(rawPayload, 200)))
                .onFailure().recoverWithItem((Void) null);
    }

    private Uni<Void> processSyncMessage(String rawPayload) {
        SyncEventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(rawPayload, SyncEventEnvelope.class);
        } catch (Exception e) {
            log.errorf(e, "Failed to deserialize sync event: %s", truncate(rawPayload, 200));
            return Uni.createFrom().voidItem();
        }

        // Safety check 1: Skip events from our own site (shouldn't happen with proper topic config, but defense-in-depth)
        if (siteConfig.id().equalsIgnoreCase(envelope.getSourceSite())) {
            log.debugf("Skipping sync event from own site: eventId=%s, source=%s",
                    envelope.getEventId(), envelope.getSourceSite());
            return Uni.createFrom().voidItem();
        }

        // Safety check 2: Skip duplicate events
        if (dedupService.isDuplicate(envelope.getEventId())) {
            log.debugf("Skipping duplicate sync event: eventId=%s", envelope.getEventId());
            return Uni.createFrom().voidItem();
        }

        DBWriteRequest request = envelope.getPayload();
        if (request == null) {
            log.warnf("Sync event has null payload: eventId=%s", envelope.getEventId());
            return Uni.createFrom().voidItem();
        }

        log.infof("Applying sync event: eventId=%s, source=%s, table=%s, eventType=%s, user=%s",
                envelope.getEventId(), envelope.getSourceSite(),
                request.getTableName(), request.getEventType(), request.getUserName());

        // Mark as processed BEFORE applying — prevents re-publish by SyncEventProducer
        dedupService.markProcessed(envelope.getEventId());

        // Apply the write to local DB — publishSync=false to prevent re-publishing
        return dbWriteService.processEventAndSync(request, false)
                .onItem().invoke(() -> {
                    long count = syncedCount.incrementAndGet();
                    if (count % 100 == 0) {
                        log.infof("Synced %d events from remote site (dedup cache size: %d)",
                                count, dedupService.size());
                    }
                })
                .onFailure().invoke(t ->
                        log.errorf(t, "Failed to apply sync event: eventId=%s, table=%s, user=%s",
                                envelope.getEventId(), request.getTableName(), request.getUserName()))
                .onFailure().recoverWithItem((Void) null);
    }

    private static String truncate(String s, int maxLen) {
        return s != null && s.length() > maxLen ? s.substring(0, maxLen) + "..." : s;
    }
}
