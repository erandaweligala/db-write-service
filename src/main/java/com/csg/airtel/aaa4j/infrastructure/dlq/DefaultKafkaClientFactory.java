package com.csg.airtel.aaa4j.infrastructure.dlq;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Production {@link KafkaClientFactory} backed by the plain Apache Kafka clients.
 *
 * <p>Deliberately independent of the SmallRye Reactive Messaging channels: the reprocessor
 * needs an on-demand, bounded drain of a DLT topic, which is cleaner with a directly
 * managed consumer than by wiring another {@code @Incoming} channel that would run
 * continuously and risk tight-looping on poison messages.
 */
@ApplicationScoped
public class DefaultKafkaClientFactory implements KafkaClientFactory {

    private static final Logger log = Logger.getLogger(DefaultKafkaClientFactory.class);

    /** Bound on each broker admin call so a slow cluster cannot stall the drain start. */
    private static final long ADMIN_TIMEOUT_SECONDS = 15;

    @ConfigProperty(name = "dlq.reprocess.bootstrap-servers", defaultValue = "localhost:9092")
    String bootstrapServers;

    @ConfigProperty(name = "dlq.reprocess.max-poll-records", defaultValue = "100")
    int maxPollRecords;

    @Override
    public Consumer<String, byte[]> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // We drive commits explicitly (capped at the run's start end-offsets) so re-queued
        // records are re-read on a later run instead of being skipped.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID());
        return new KafkaConsumer<>(props);
    }

    @Override
    public Producer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Durability first: a re-queued/parked record must survive before we commit the
        // consumer offset that made it disappear from the source DLT.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Fail fast instead of blocking send() for the default 60s when a target topic is
        // missing (UNKNOWN_TOPIC_OR_PARTITION) — the run aborts cleanly rather than hanging
        // through its whole time budget.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dlq-reprocessor-producer-" + UUID.randomUUID());
        return new KafkaProducer<>(props);
    }

    /**
     * Creates {@code parkedTopic} if it does not exist yet, mirroring the source DLT's
     * partition count and replication factor so parked records keep the same key
     * distribution. Never throws: on clusters where the reprocessor lacks topic-create
     * permission this degrades to a WARN, and a genuinely failed park still aborts the
     * run before the offset commit.
     */
    @Override
    public void ensureParkedTopic(String sourceTopic, String parkedTopic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "dlq-reprocessor-admin-" + UUID.randomUUID());
        try (Admin admin = Admin.create(props)) {
            if (admin.listTopics().names().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .contains(parkedTopic)) {
                return; // already provisioned
            }
            TopicDescription source = admin.describeTopics(List.of(sourceTopic))
                    .allTopicNames().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get(sourceTopic);
            int partitions = source.partitions().size();
            short replication = (short) source.partitions().get(0).replicas().size();
            admin.createTopics(List.of(new NewTopic(parkedTopic, partitions, replication)))
                    .all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LoggingUtil.logInfo(log, "ensureParkedTopic",
                    "Created parked topic %s (partitions=%d, replication=%d) mirroring %s",
                    parkedTopic, partitions, replication, sourceTopic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return; // raced with another creator — the topic exists, which is all we need
            }
            LoggingUtil.logWarn(log, "ensureParkedTopic",
                    "Could not verify/create parked topic %s: %s — a park during this run will abort it instead of losing the record",
                    parkedTopic, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LoggingUtil.logWarn(log, "ensureParkedTopic",
                    "Interrupted while ensuring parked topic %s", parkedTopic);
        } catch (Exception e) {
            LoggingUtil.logWarn(log, "ensureParkedTopic",
                    "Could not verify/create parked topic %s: %s — a park during this run will abort it instead of losing the record",
                    parkedTopic, e.getMessage());
        }
    }
}
