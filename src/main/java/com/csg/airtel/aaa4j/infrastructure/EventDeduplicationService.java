package com.csg.airtel.aaa4j.infrastructure;

import com.csg.airtel.aaa4j.application.config.SiteConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * In-memory deduplication service for cross-site sync events.
 *
 * Prevents infinite loops: when site DC writes and publishes to DC-DR,
 * site DR consumes and applies the write, but must NOT re-publish it
 * back to DR-DC. The eventId is tracked so we know it's already been synced.
 *
 * Uses a bounded ConcurrentHashMap with FIFO eviction to cap memory usage.
 * TTL-based expiry is handled by checking timestamps on access.
 */
@ApplicationScoped
public class EventDeduplicationService {

    private static final Logger log = Logger.getLogger(EventDeduplicationService.class);

    private final ConcurrentHashMap<String, Long> seenEvents = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> evictionQueue = new ConcurrentLinkedQueue<>();
    private final int maxSize;
    private final long ttlMillis;

    @Inject
    public EventDeduplicationService(SiteConfig siteConfig) {
        this.maxSize = siteConfig.dedupMaxSize();
        this.ttlMillis = siteConfig.dedupTtlSeconds() * 1000L;
        log.infof("EventDeduplicationService initialized: maxSize=%d, ttlSeconds=%d", maxSize, siteConfig.dedupTtlSeconds());
    }

    /**
     * Check if this eventId has already been seen (i.e., already applied locally).
     * Returns true if this is a DUPLICATE that should be skipped.
     */
    public boolean isDuplicate(String eventId) {
        if (eventId == null) return false;

        Long timestamp = seenEvents.get(eventId);
        if (timestamp == null) return false;

        // Check TTL expiry
        if (System.currentTimeMillis() - timestamp > ttlMillis) {
            seenEvents.remove(eventId);
            return false;
        }
        return true;
    }

    /**
     * Mark an eventId as processed. Call this AFTER successfully applying
     * a synced write to the local DB, so it won't be re-published.
     */
    public void markProcessed(String eventId) {
        if (eventId == null) return;

        seenEvents.put(eventId, System.currentTimeMillis());
        evictionQueue.add(eventId);

        // Evict oldest entries if over capacity
        while (seenEvents.size() > maxSize) {
            String oldest = evictionQueue.poll();
            if (oldest != null) {
                seenEvents.remove(oldest);
            }
        }
    }

    /**
     * Get the current cache size (for metrics/debugging).
     */
    public int size() {
        return seenEvents.size();
    }
}
