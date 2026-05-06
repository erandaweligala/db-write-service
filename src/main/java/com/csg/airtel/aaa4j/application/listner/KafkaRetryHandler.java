package com.csg.airtel.aaa4j.application.listner;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;

/**
 * Republishes messages that failed at the application/code level back to the
 * Kafka queue so they can be reprocessed. A retry-count header is incremented
 * on each republish; once {@code app.kafka.max-retries} is exceeded the
 * message is routed to the dead-letter topic instead.
 */
@ApplicationScoped
public class KafkaRetryHandler {

    private static final Logger log = Logger.getLogger(KafkaRetryHandler.class);

    public static final String RETRY_COUNT_HEADER = "x-retry-count";
    public static final String LAST_ERROR_HEADER = "x-last-error";
    public static final String ORIGINAL_TOPIC_HEADER = "x-original-topic";

    @ConfigProperty(name = "app.kafka.max-retries", defaultValue = "3")
    int maxRetries;

    @ConfigProperty(name = "app.kafka.dlq-topic", defaultValue = "db-write-dlq")
    String dlqTopic;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    @Channel("retry-out")
    MutinyEmitter<String> retryEmitter;

    /**
     * Republish the failed message to its original topic so it returns to the
     * queue. After max retries it is routed to the DLQ topic instead.
     *
     * @param message  the original incoming message
     * @param topic    the topic to republish to (typically the source topic)
     * @param payload  the payload to republish — String is sent as-is, anything
     *                 else is JSON-serialized.
     * @param error    the exception that caused the failure
     */
    public <T> Uni<Void> requeueOnFailure(Message<?> message, String topic, T payload, Throwable error) {
        int retryCount = readRetryCount(message);
        int nextAttempt = retryCount + 1;

        boolean toDlq = nextAttempt > maxRetries;
        String targetTopic = toDlq ? dlqTopic : topic;

        if (toDlq) {
            LoggingUtil.logError(log, "requeueOnFailure", error,
                    "Max retries (%d) exceeded for topic=%s — routing to DLQ %s",
                    maxRetries, topic, dlqTopic);
        } else {
            LoggingUtil.logWarn(log, "requeueOnFailure",
                    "Republishing to topic=%s (attempt %d/%d) due to: %s",
                    topic, nextAttempt, maxRetries, error.getMessage());
        }

        String body;
        try {
            body = (payload instanceof String s) ? s : objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            LoggingUtil.logError(log, "requeueOnFailure", ex,
                    "Failed to serialize payload for republish — dropping. topic=%s", topic);
            return Uni.createFrom().voidItem();
        }

        RecordHeaders headers = new RecordHeaders();
        headers.add(RETRY_COUNT_HEADER, String.valueOf(nextAttempt).getBytes(StandardCharsets.UTF_8));
        headers.add(ORIGINAL_TOPIC_HEADER, topic.getBytes(StandardCharsets.UTF_8));
        headers.add(LAST_ERROR_HEADER, safeErrorMessage(error).getBytes(StandardCharsets.UTF_8));

        OutgoingKafkaRecordMetadata<Object> meta = OutgoingKafkaRecordMetadata.builder()
                .withTopic(targetTopic)
                .withHeaders(headers)
                .build();

        return retryEmitter.sendMessage(Message.of(body).addMetadata(meta))
                .onFailure().invoke(t -> LoggingUtil.logError(log, "requeueOnFailure", t,
                        "Failed to republish message to topic=%s", targetTopic))
                .onFailure().recoverWithItem((Void) null);
    }

    private int readRetryCount(Message<?> message) {
        return message.getMetadata(IncomingKafkaRecordMetadata.class)
                .map(md -> {
                    Header h = md.getHeaders().lastHeader(RETRY_COUNT_HEADER);
                    if (h == null || h.value() == null) return 0;
                    try {
                        return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8).trim());
                    } catch (NumberFormatException nfe) {
                        return 0;
                    }
                })
                .orElse(0);
    }

    private String safeErrorMessage(Throwable error) {
        String msg = error.getMessage();
        if (msg == null) msg = error.getClass().getName();
        if (msg.length() > 1024) msg = msg.substring(0, 1024);
        return msg;
    }
}
