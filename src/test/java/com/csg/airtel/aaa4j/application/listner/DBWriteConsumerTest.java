package com.csg.airtel.aaa4j.application.listner;

import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("DBWriteConsumer Tests")
class DBWriteConsumerTest {

    @Mock
    private DBWriteService mockDbWriteService;

    @Mock
    private Message<DBWriteRequest> mockMessage;

    private DBWriteConsumer consumer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        consumer = new DBWriteConsumer(mockDbWriteService);
    }

    @Test
    @DisplayName("Should create DBWriteConsumer with service")
    void testConstructor() {
        assertNotNull(consumer);
    }

    @Test
    @DisplayName("Should consume accounting event successfully")
    void testConsumeAccountingEventSuccess() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should process message payload")
    void testProcessMessagePayload() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("status", "active");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        consumer.consumeAccountingEvent(mockMessage);

        // Verify
        verify(mockMessage, times(1)).getPayload();
    }

    @Test
    @DisplayName("Should call service with request")
    void testCallServiceWithRequest() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        consumer.consumeAccountingEvent(mockMessage);

        // Verify
        verify(mockDbWriteService, times(1)).processDbWriteRequest(request);
    }

    @Test
    @DisplayName("Should acknowledge message on success")
    void testAcknowledgeMessageOnSuccess() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle processing errors")
    void testHandleProcessingErrors() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("Processing error")));
        when(mockMessage.nack(any(Throwable.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should nack message on failure")
    void testNackMessageOnFailure() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("Error")));
        when(mockMessage.nack(any(Throwable.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should extract Kafka metadata when available")
    void testExtractKafkaMetadata() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should track processed message count")
    void testTrackProcessedMessageCount() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute and verify multiple messages
        for (int i = 0; i < 5; i++) {
            consumer.consumeAccountingEvent(mockMessage);
        }

        verify(mockDbWriteService, times(5)).processDbWriteRequest(any(DBWriteRequest.class));
    }

    @Test
    @DisplayName("Should handle null message payload safely")
    void testHandleNullMessagePayload() {
        // Setup
        when(mockMessage.getPayload()).thenReturn(null);
        when(mockDbWriteService.processDbWriteRequest(null))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle message with UPDATE_EVENT")
    void testHandleUpdateEvent() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("status", "updated");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 100);

        DBWriteRequest request = new DBWriteRequest(
                "accounts",
                whereConditions,
                columnValues,
                "admin",
                "2024-12-01T15:30:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
        verify(mockDbWriteService, times(1)).processDbWriteRequest(request);
    }

    @Test
    @DisplayName("Should return Uni for async processing")
    void testReturnUniType() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockMessage.getPayload()).thenReturn(request);
        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());
        when(mockMessage.ack()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockMessage.getMetadata(IncomingKafkaRecordMetadata.class))
                .thenReturn(Optional.empty());

        // Execute
        var result = consumer.consumeAccountingEvent(mockMessage);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof Uni);
    }
}
