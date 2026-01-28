package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("DBWriteService Tests")
class DBWriteServiceTest {

    @Mock
    private DBWriteRepository mockRepository;

    @Mock
    private DBOperationsService dbOperationsService;

    private DBWriteService service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        service = new DBWriteService(mockRepository,dbOperationsService);
    }

    @Test
    @DisplayName("Should create DBWriteService with repository")
    void testConstructor() {
        assertNotNull(service);
    }

    @Test
    @DisplayName("Should process UPDATE_EVENT request")
    void testProcessUpdateEvent() {
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

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, times(1)).update("users", columnValues, whereConditions);
    }

    @Test
    @DisplayName("Should process update_event in lowercase")
    void testProcessUpdateEventLowercase() {
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
                "update_event"
        );

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, times(1)).update("users", columnValues, whereConditions);
    }

    @Test
    @DisplayName("Should process UpdateEvent mixed case")
    void testProcessUpdateEventMixedCase() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("age", 25);

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 2);

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UpDaTe_EvEnT"
        );

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, times(1)).update("users", columnValues, whereConditions);
    }

    @Test
    @DisplayName("Should return empty Uni for non-UPDATE_EVENT")
    void testProcessNonUpdateEvent() {
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
                "CREATE_EVENT"
        );

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, never()).update(anyString(), any(Map.class), any(Map.class));
    }

    @Test
    @DisplayName("Should return empty Uni for DELETE_EVENT")
    void testProcessDeleteEvent() {
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
                "DELETE_EVENT"
        );

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, never()).update(anyString(), any(Map.class), any(Map.class));
    }

    @Test
    @DisplayName("Should return empty Uni for INSERT_EVENT")
    void testProcessInsertEvent() {
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
                "INSERT_EVENT"
        );

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, never()).update(anyString(), any(Map.class), any(Map.class));
    }

    @Test
    @DisplayName("Should return empty Uni for unknown event type")
    void testProcessUnknownEventType() {
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
                "UNKNOWN_EVENT"
        );

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, never()).update(anyString(), any(Map.class), any(Map.class));
    }

    @Test
    @DisplayName("Should handle null event type")
    void testProcessNullEventType() {
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
                null
        );

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, never()).update(anyString(), any(Map.class), any(Map.class));
    }

    @Test
    @DisplayName("Should handle empty event type")
    void testProcessEmptyEventType() {
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
                ""
        );

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, never()).update(anyString(), any(Map.class), any(Map.class));
    }

    @Test
    @DisplayName("Should pass correct table name to repository")
    void testTableNamePassthrough() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("col1", "value1");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "products",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        service.processDbWriteRequest(request);

        // Verify
        verify(mockRepository, times(1)).update("products", columnValues, whereConditions);
    }

    @Test
    @DisplayName("Should pass correct column values to repository")
    void testColumnValuesPassthrough() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("col1", "value1");
        columnValues.put("col2", "value2");

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

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(2));

        // Execute
        service.processDbWriteRequest(request);

        // Verify
        verify(mockRepository, times(1)).update("users", columnValues, whereConditions);
    }

    @Test
    @DisplayName("Should pass correct where conditions to repository")
    void testWhereConditionsPassthrough() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("status", "inactive");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);
        whereConditions.put("type", "premium");

        DBWriteRequest request = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        service.processDbWriteRequest(request);

        // Verify
        verify(mockRepository, times(1)).update("users", columnValues, whereConditions);
    }

    @Test
    @DisplayName("Should handle repository failure")
    void testRepositoryFailure() {
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

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("DB Error")));

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, times(1)).update("users", columnValues, whereConditions);
    }

    @Test
    @DisplayName("Should handle multiple sequential requests")
    void testMultipleSequentialRequests() {
        // Setup
        Map<String, Object> columnValues1 = new HashMap<>();
        columnValues1.put("name", "John");

        Map<String, Object> whereConditions1 = new HashMap<>();
        whereConditions1.put("id", 1);

        DBWriteRequest request1 = new DBWriteRequest(
                "users",
                whereConditions1,
                columnValues1,
                "user1",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        Map<String, Object> columnValues2 = new HashMap<>();
        columnValues2.put("name", "Jane");

        Map<String, Object> whereConditions2 = new HashMap<>();
        whereConditions2.put("id", 2);

        DBWriteRequest request2 = new DBWriteRequest(
                "users",
                whereConditions2,
                columnValues2,
                "user2",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        service.processDbWriteRequest(request1);
        service.processDbWriteRequest(request2);

        // Verify
        verify(mockRepository, times(2)).update(anyString(), any(Map.class), any(Map.class));
    }

    @Test
    @DisplayName("Should return Uni type for async processing")
    void testReturnsUniType() {
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

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof Uni);
    }

    @Test
    @DisplayName("Should handle UPPERCASE UPDATE_EVENT")
    void testUppercaseUpdateEvent() {
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

        when(mockRepository.update(anyString(), any(Map.class), any(Map.class)))
                .thenReturn(Uni.createFrom().item(1));

        // Execute
        var result = service.processDbWriteRequest(request);

        // Verify
        assertNotNull(result);
        verify(mockRepository, times(1)).update(anyString(), any(Map.class), any(Map.class));
    }
}
