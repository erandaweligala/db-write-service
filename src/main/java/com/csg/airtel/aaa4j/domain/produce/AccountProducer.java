package com.csg.airtel.aaa4j.domain.produce;

import com.csg.airtel.aaa4j.domain.model.AccountingResponseEvent;
import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.model.QuotaNotificationEvent;
import com.csg.airtel.aaa4j.domain.model.cdr.AccountingCDREvent;
import com.csg.airtel.aaa4j.domain.service.FailoverPathLogger;
import com.csg.airtel.aaa4j.domain.service.SessionLifecycleManager;
import com.csg.airtel.aaa4j.external.clients.CacheClient;
import com.csg.airtel.aaa4j.domain.model.session.Session;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletableFuture;


@ApplicationScoped
public class AccountProducer {

    private static final Logger LOG = Logger.getLogger(AccountProducer.class);

    // =========================================================================
    // Site identity: determines which Kafka topic to publish DB writes to.
    //   DC site → publishes to "DC-DR" topic
    //   DR site → publishes to "DR-DC" topic
    // =========================================================================
    @ConfigProperty(name = "app.site", defaultValue = "DC")
    String site;

    private String dbWriteTopic;

    private final Emitter<DBWriteRequest> dbWriteRequestEmitter;
    private final Emitter<AccountingResponseEvent> accountingResponseEmitter;
    private final Emitter<AccountingCDREvent> accountingCDREventEmitter;
    private final Emitter<QuotaNotificationEvent> quotaNotificationEmitter;
    private final CacheClient cacheClient;
    private final SessionLifecycleManager sessionLifecycleManager;

    public AccountProducer(@Channel("db-write-events") Emitter<DBWriteRequest> dbWriteRequestEmitter,
                           @Channel("accounting-resp-events") Emitter<AccountingResponseEvent> accountingResponseEmitter,
                           @Channel("accounting-cdr-events") Emitter<AccountingCDREvent> accountingCDREventEmitter,
                           @Channel("quota-notification-events") Emitter<QuotaNotificationEvent> quotaNotificationEmitter,
                           CacheClient cacheClient,
                           SessionLifecycleManager sessionLifecycleManager
    ) {
        this.dbWriteRequestEmitter = dbWriteRequestEmitter;
        this.accountingResponseEmitter = accountingResponseEmitter;
        this.accountingCDREventEmitter = accountingCDREventEmitter;
        this.quotaNotificationEmitter = quotaNotificationEmitter;
        this.cacheClient = cacheClient;
        this.sessionLifecycleManager = sessionLifecycleManager;
    }

    @PostConstruct
    void init() {
        // DC site publishes to DC-DR topic, DR site publishes to DR-DC topic
        dbWriteTopic = "DC".equalsIgnoreCase(site) ? "DC-DR" : "DR-DC";
        LOG.infof("AccountProducer initialized: site=%s, dbWriteTopic=%s", site, dbWriteTopic);
    }

    // =========================================================================
    // DB Write Event — publishes to DC-DR or DR-DC based on site
    // =========================================================================

    @CircuitBreaker(
            requestVolumeThreshold = 200,
            failureRatio = 0.75,
            delay = 3000,
            successThreshold = 3
    )
    @Retry(
            maxRetries = 3,
            delay = 100,
            maxDuration = 10000
    )
    @Timeout(value = 10000)
    @Fallback(fallbackMethod = "fallbackProduceDBWriteEvent")
    public Uni<Void> produceDBWriteEvent(DBWriteRequest request) {
        long startTime = System.currentTimeMillis();
        LOG.infof("[%s] Start produceDBWriteEvent → topic=%s, sessionId=%s",
                site, dbWriteTopic, request.getSessionId());

        return Uni.createFrom().emitter(em -> {
            Message<DBWriteRequest> message = Message.of(request)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                            .withKey(request.getSessionId())
                            .withTopic(dbWriteTopic)       // <-- DYNAMIC TOPIC based on site
                            .build())
                    .withAck(() -> {
                        em.complete(null);
                        LOG.infof("[%s] Sent DB write event → topic=%s, session=%s, %d ms",
                                site, dbWriteTopic, request.getSessionId(),
                                System.currentTimeMillis() - startTime);
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(throwable -> {
                        LOG.errorf("[%s] DB write send failed → topic=%s: %s",
                                site, dbWriteTopic, throwable.getMessage());
                        em.fail(throwable);
                        return CompletableFuture.completedFuture(null);
                    });

            dbWriteRequestEmitter.send(message);
        });
    }

    // =========================================================================
    // Accounting Response Event
    // =========================================================================

    @CircuitBreaker(
            requestVolumeThreshold = 200,
            failureRatio = 0.75,
            delay = 3000,
            successThreshold = 3
    )
    @Retry(
            maxRetries = 3,
            delay = 100,
            maxDuration = 10000
    )
    @Timeout(value = 10000)
    @Fallback(fallbackMethod = "fallbackProduceAccountingResponseEvent")
    public Uni<Void> produceAccountingResponseEvent(AccountingResponseEvent event) {
        long startTime = System.currentTimeMillis();
        LOG.infof("[%s] Start produceAccountingResponseEvent", site);
        return Uni.createFrom().emitter(em -> {
            Message<AccountingResponseEvent> message = Message.of(event)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                            .withKey(event.sessionId())
                            .build())
                    .withAck(() -> {
                        em.complete(null);
                        LOG.infof("[%s] Sent accounting response event session=%s, %d ms",
                                site, event.sessionId(), System.currentTimeMillis() - startTime);
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(throwable -> {
                        LOG.errorf("[%s] Accounting response send failed: %s",
                                site, throwable.getMessage());
                        em.fail(throwable);
                        return CompletableFuture.completedFuture(null);
                    });

            accountingResponseEmitter.send(message);
        });
    }

    // =========================================================================
    // Accounting CDR Event
    // =========================================================================

    @CircuitBreaker(
            requestVolumeThreshold = 200,
            failureRatio = 0.75,
            delay = 3000,
            successThreshold = 3
    )
    @Retry(
            maxRetries = 3,
            delay = 100,
            maxDuration = 10000
    )
    @Timeout(value = 10000)
    @Fallback(fallbackMethod = "fallbackProduceAccountingCDREvent")
    public Uni<Void> produceAccountingCDREvent(AccountingCDREvent event) {
        long startTime = System.currentTimeMillis();
        LOG.infof("[%s] Start produce Accounting CDR Event", site);
        return Uni.createFrom().emitter(em -> {
            Message<AccountingCDREvent> message = Message.of(event)
                    .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                            .withKey(event.getPayload().getSession().getSessionId())
                            .build())
                    .withAck(() -> {
                        em.complete(null);
                        LOG.infof("[%s] Sent accounting CDR event session=%s, %d ms",
                                site, event.getPayload().getSession().getSessionId(),
                                System.currentTimeMillis() - startTime);
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(throwable -> {
                        LOG.errorf("[%s] CDR send failed: %s", site, throwable.getMessage());
                        em.fail(throwable);
                        return CompletableFuture.completedFuture(null);
                    });

            accountingCDREventEmitter.send(message);
        });
    }

    // =========================================================================
    // Quota Notification Event
    // =========================================================================

    @CircuitBreaker(
            requestVolumeThreshold = 200,
            failureRatio = 0.75,
            delay = 3000,
            successThreshold = 3
    )
    @Retry(
            maxRetries = 3,
            delay = 100,
            maxDuration = 10000
    )
    @Timeout(value = 10000)
    @Fallback(fallbackMethod = "fallbackProduceQuotaNotificationEvent")
    public Uni<Void> produceQuotaNotificationEvent(QuotaNotificationEvent event) {
        LOG.infof("[%s] Start produce Quota Notification Event user=%s, type=%s",
                site, event.username(), event.type());
        return Uni.createFrom().voidItem();
    }

    // =========================================================================
    // Fallbacks
    // =========================================================================

    private Uni<Void> fallbackProduceDBWriteEvent(DBWriteRequest request, Throwable throwable) {
        FailoverPathLogger.logFallbackPath(LOG, "produceDBWriteEvent", request.getSessionId(), throwable);

        String userId = request.getUserName();
        String sessionId = request.getSessionId();

        LOG.infof("[%s] Initiating session revoke fallback for userId=%s, sessionId=%s",
                site, userId, sessionId);

        return cacheClient.getUserData(userId)
                .onItem().transformToUni(userSessionData -> {
                    if (userSessionData == null) {
                        LOG.debugf("No user session data found for userId=%s during fallback revoke", userId);
                        return Uni.createFrom().voidItem();
                    }

                    Session sessionToRemove = null;
                    if (userSessionData.getSessions() != null) {
                        for (Session session : userSessionData.getSessions()) {
                            if (sessionId.equals(session.getSessionId())) {
                                sessionToRemove = session;
                                break;
                            }
                        }
                    }

                    if (sessionToRemove != null) {
                        userSessionData.getSessions().remove(sessionToRemove);
                        LOG.infof("Removed session %s from user %s sessions during fallback", sessionId, userId);

                        return cacheClient.updateUserAndRelatedCaches(userId, userSessionData, request.getUserName())
                                .call(() -> sessionLifecycleManager.onSessionTerminated(userId, sessionId))
                                .invoke(() -> LOG.infof("Session revoke fallback completed userId=%s, sessionId=%s",
                                        userId, sessionId))
                                .onFailure().invoke(e -> LOG.errorf(e,
                                        "Failed to complete session revoke fallback userId=%s, sessionId=%s",
                                        userId, sessionId))
                                .onFailure().recoverWithNull()
                                .replaceWithVoid();
                    } else {
                        LOG.debugf("Session %s not found in user %s sessions during fallback revoke",
                                sessionId, userId);
                        return sessionLifecycleManager.onSessionTerminated(userId, sessionId);
                    }
                })
                .onFailure().invoke(e -> LOG.errorf(e,
                        "Failed to retrieve user data during session revoke fallback userId=%s, sessionId=%s",
                        userId, sessionId))
                .onFailure().recoverWithNull()
                .replaceWithVoid();
    }

    private Uni<Void> fallbackProduceAccountingResponseEvent(AccountingResponseEvent event, Throwable throwable) {
        FailoverPathLogger.logFallbackPath(LOG, "produceAccountingResponseEvent", event.sessionId(), throwable);
        return Uni.createFrom().voidItem();
    }

    private Uni<Void> fallbackProduceAccountingCDREvent(AccountingCDREvent event, Throwable throwable) {
        String sessionId = event.getPayload() != null && event.getPayload().getSession() != null
                ? event.getPayload().getSession().getSessionId()
                : "unknown";
        FailoverPathLogger.logFallbackPath(LOG, "produceAccountingCDREvent", sessionId, throwable);
        return Uni.createFrom().voidItem();
    }

    private Uni<Void> fallbackProduceQuotaNotificationEvent(QuotaNotificationEvent event, Throwable throwable) {
        FailoverPathLogger.logFallbackPath(LOG, "produceQuotaNotificationEvent", event.username(), throwable);
        return Uni.createFrom().voidItem();
    }
}
