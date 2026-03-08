package com.csg.airtel.aaa4j.application.listner;

import com.csg.airtel.aaa4j.application.config.SiteConfig;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.model.SyncEventEnvelope;
import com.csg.airtel.aaa4j.infrastructure.EventDeduplicationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Publishes local DB write events to the outbound Kafka sync topic
 * so the remote site can replicate the change.
 *
 * DC site publishes to "sync-outbound" channel → mapped to DC-DR topic
 * DR site publishes to "sync-outbound" channel → mapped to DR-DC topic
 *
 * The topic name is configured per-site in application.yml, so the same
 * code runs on both sites — only config differs.
 */
@ApplicationScoped
public class SyncEventProducer {

    private static final Logger log = Logger.getLogger(SyncEventProducer.class);

    private final SiteConfig siteConfig;
    private final EventDeduplicationService dedupService;
    private final ObjectMapper objectMapper;

    @Inject
    @Channel("sync-outbound")
    MutinyEmitter<String> syncEmitter;

    @Inject
    public SyncEventProducer(SiteConfig siteConfig,
                             EventDeduplicationService dedupService,
                             ObjectMapper objectMapper) {
        this.siteConfig = siteConfig;
        this.dedupService = dedupService;
        this.objectMapper = objectMapper;
    }

    /**
     * Publish a DB write event to the remote site for replication.
     *
     * @param request  the DB write that was applied locally
     * @param eventId  if non-null, this event was received from a remote site
     *                 and should NOT be re-published (prevents infinite loop)
     * @return Uni<Void> that completes when the message is sent
     */
    public Uni<Void> publishForSync(DBWriteRequest request, String eventId) {
        // If sync is disabled, skip
        if (!siteConfig.syncEnabled()) {
            log.debugf("Sync disabled — skipping publish for table=%s", request.getTableName());
            return Uni.createFrom().voidItem();
        }

        // If this event came from remote (has an eventId that we've seen), don't re-publish
        if (eventId != null && dedupService.isDuplicate(eventId)) {
            log.debugf("Skipping re-publish of synced event: eventId=%s", eventId);
            return Uni.createFrom().voidItem();
        }

        // Create envelope with new unique eventId
        String newEventId = UUID.randomUUID().toString();
        SyncEventEnvelope envelope = SyncEventEnvelope.builder()
                .eventId(newEventId)
                .sourceSite(siteConfig.id())
                .createdAt(Instant.now().toString())
                .payload(request)
                .version(System.currentTimeMillis())
                .build();

        // Mark this event so if it echoes back, we skip it
        dedupService.markProcessed(newEventId);

        try {
            String json = objectMapper.writeValueAsString(envelope);

            // Use userName as Kafka key for partition affinity (same user → same partition → ordering)
            String key = request.getUserName() != null ? request.getUserName() : "unknown";

            OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                    .withKey(key)
                    .withHeaders(new RecordHeaders()
                            .add("sync-event-id", newEventId.getBytes(StandardCharsets.UTF_8))
                            .add("sync-source-site", siteConfig.id().getBytes(StandardCharsets.UTF_8)))
                    .build();

            Message<String> message = Message.of(json).addMetadata(metadata);

            log.infof("Publishing sync event: eventId=%s, site=%s, table=%s, eventType=%s, user=%s",
                    newEventId, siteConfig.id(), request.getTableName(),
                    request.getEventType(), request.getUserName());

            return syncEmitter.sendMessage(message);

        } catch (Exception e) {
            log.errorf(e, "Failed to serialize sync event for table=%s, user=%s",
                    request.getTableName(), request.getUserName());
            return Uni.createFrom().failure(e);
        }
    }

    /**
     * Convenience method for local writes (no incoming eventId).
     */
    public Uni<Void> publishForSync(DBWriteRequest request) {
        return publishForSync(request, null);
    }
}
