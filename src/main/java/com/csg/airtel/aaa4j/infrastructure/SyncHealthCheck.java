package com.csg.airtel.aaa4j.infrastructure;

import com.csg.airtel.aaa4j.application.config.SiteConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/**
 * Health check for bidirectional sync status.
 * Reports site identity, sync enabled status, and dedup cache size.
 */
@Readiness
@ApplicationScoped
public class SyncHealthCheck implements HealthCheck {

    @Inject
    SiteConfig siteConfig;

    @Inject
    EventDeduplicationService dedupService;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("cross-site-sync")
                .status(true)
                .withData("site.id", siteConfig.id())
                .withData("sync.enabled", siteConfig.syncEnabled())
                .withData("dedup.cache.size", dedupService.size())
                .withData("dedup.max.size", siteConfig.dedupMaxSize())
                .build();
    }
}
