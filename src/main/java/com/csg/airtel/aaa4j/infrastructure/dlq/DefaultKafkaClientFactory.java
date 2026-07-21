package com.csg.airtel.aaa4j.infrastructure.dlq;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Properties;
import java.util.UUID;

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
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dlq-reprocessor-producer-" + UUID.randomUUID());
        return new KafkaProducer<>(props);
    }
}
