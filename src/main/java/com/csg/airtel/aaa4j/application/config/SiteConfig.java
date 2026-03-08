package com.csg.airtel.aaa4j.application.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration for bidirectional site sync.
 *
 * Each deployment sets its own site identity:
 *   - Primary datacenter: site.id=DC
 *   - DR datacenter:      site.id=DR
 *
 * The sync.enabled flag allows disabling cross-site replication
 * without redeploying (e.g., during maintenance or initial data load).
 */
@ConfigMapping(prefix = "site")
public interface SiteConfig {

    /** This site's identity: "DC" or "DR" */
    @WithDefault("DC")
    String id();

    /** Enable/disable cross-site sync */
    @WithDefault("true")
    boolean syncEnabled();

    /** Deduplication cache TTL in seconds (how long to remember processed eventIds) */
    @WithDefault("300")
    int dedupTtlSeconds();

    /** Maximum dedup cache size to prevent unbounded memory growth */
    @WithDefault("100000")
    int dedupMaxSize();
}
