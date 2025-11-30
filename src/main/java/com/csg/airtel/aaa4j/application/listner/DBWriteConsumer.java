package com.csg.airtel.aaa4j.application.listner;


import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;


@ApplicationScoped
public class DBWriteConsumer {
    private static final Logger log = Logger.getLogger(DBWriteConsumer.class);

    final DBWriteService dbWriteService;

    @Inject
    public DBWriteConsumer(DBWriteService dbWriteService) {
        this.dbWriteService = dbWriteService;
    }

    @Incoming("db-write-events")
    public Uni<Void> consumeAccountingEvent(Message<DBWriteRequest> message) {
        log.infof("DB write event received");
        DBWriteRequest request = message.getPayload();

        // Log partition info
        if(log.isInfoEnabled()) {
            message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .ifPresent(metadata -> log.infof("Received from partition: %d, offset: %d, key: %s",
                            metadata.getPartition(),
                            metadata.getOffset(),
                            metadata.getKey()));
        }

        return dbWriteService.processDbWriteRequest(request)
                .onItem().transformToUni(result -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(throwable -> {
                    log.errorf(throwable, "Error processing event for user: %s | eventType: %s",
                            request.getUserName(), request.getEventType());
                    return Uni.createFrom().completionStage(message.nack(throwable));
                });
    }
}
