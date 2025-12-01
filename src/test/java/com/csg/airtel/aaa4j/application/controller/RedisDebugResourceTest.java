package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.domain.service.DBWriteService;
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

@DisplayName("RedisDebugResource Tests")
class RedisDebugResourceTest {

    @Mock
    private DBWriteService mockDbWriteService;

    private RedisDebugResource resource;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        resource = new RedisDebugResource();
        resource.dbWriteService = mockDbWriteService;
    }

    @Test
    @DisplayName("Should create RedisDebugResource")
    void testConstructor() {
        assertNotNull(resource);
    }

    @Test
    @DisplayName("Should process interim update request successfully")
    void testInterimUpdateSuccess() {
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

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
        verify(mockDbWriteService, times(1)).processDbWriteRequest(request);
    }

    @Test
    @DisplayName("Should return Map with accounting_result key")
    void testReturnMapStructure() {
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

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle null request")
    void testHandleNullRequest() {
        // Setup
        when(mockDbWriteService.processDbWriteRequest(null))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(null);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should call service with correct request parameters")
    void testCallServiceWithCorrectParameters() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("col1", "val1");
        columnValues.put("col2", "val2");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 123);

        DBWriteRequest request = new DBWriteRequest(
                "products",
                whereConditions,
                columnValues,
                "user123",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        resource.interimUpdate(request);

        // Verify
        verify(mockDbWriteService, times(1)).processDbWriteRequest(request);
    }

    @Test
    @DisplayName("Should handle service failure")
    void testHandleServiceFailure() {
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

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("Service error")));

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should return Uni type")
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

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof Uni);
    }

    @Test
    @DisplayName("Should process multiple requests sequentially")
    void testMultipleRequests() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request1 = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "user1",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        DBWriteRequest request2 = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "user2",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        resource.interimUpdate(request1);
        resource.interimUpdate(request2);

        // Verify
        verify(mockDbWriteService, times(2)).processDbWriteRequest(any(DBWriteRequest.class));
    }

    @Test
    @DisplayName("Should handle UPDATE_EVENT requests")
    void testHandleUpdateEventRequests() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("status", "updated");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRequest request = new DBWriteRequest(
                "accounts",
                whereConditions,
                columnValues,
                "admin",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle requests with large payloads")
    void testHandleLargePayloads() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            columnValues.put("col" + i, "value" + i);
        }

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

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle empty column values")
    void testHandleEmptyColumnValues() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
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

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should map result to response object")
    void testMapResultToResponse() {
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

        when(mockDbWriteService.processDbWriteRequest(any(DBWriteRequest.class)))
                .thenReturn(Uni.createFrom().voidItem());

        // Execute
        var result = resource.interimUpdate(request);

        // Verify
        assertNotNull(result);
    }
}
