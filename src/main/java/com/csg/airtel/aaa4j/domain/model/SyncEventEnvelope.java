package com.csg.airtel.aaa4j.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Envelope that wraps a DBWriteRequest for cross-site Kafka sync.
 * Contains metadata to prevent infinite sync loops between DC and DR.
 *
 * Flow:
 *   1. Local write happens → SyncEventEnvelope created with unique eventId + sourceSite
 *   2. Published to outbound topic (e.g., DC-DR)
 *   3. Remote site consumes, checks eventId for dedup, applies write locally
 *   4. Remote site does NOT re-publish this event (synced=true flag / source check)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SyncEventEnvelope {

    /** Unique event ID for deduplication (UUID) */
    private String eventId;

    /** Origin site: "DC" or "DR" */
    private String sourceSite;

    /** ISO-8601 timestamp when the event was created */
    private String createdAt;

    /** The actual DB write request to replicate */
    private DBWriteRequest payload;

    /** Monotonically increasing version for conflict resolution (last-write-wins) */
    private long version;
}
