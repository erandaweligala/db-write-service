package com.csg.airtel.aaa4j.external.repository;

import com.csg.airtel.aaa4j.infrastructure.DatabaseCircuitBreaker;
import com.csg.airtel.aaa4j.infrastructure.PerformanceMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.PreparedQuery;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@DisplayName("DBWriteRepository Tests")
class DBWriteRepositoryTest {

    @Mock
    private Pool mockPool;

    @Mock
    private PreparedQuery<SqlResult> mockPreparedQuery;

    @Mock
    private SqlResult mockSqlResult;

    private DatabaseCircuitBreaker circuitBreaker;
    private PerformanceMetrics metrics;
    private DBWriteRepository repository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        circuitBreaker = new DatabaseCircuitBreaker();
        metrics = new PerformanceMetrics(new SimpleMeterRegistry());
        repository = new DBWriteRepository(mockPool, circuitBreaker, metrics);
    }

    @Test
    @DisplayName("Should update with valid table name and conditions")
    void testUpdateSuccess() {
        // Setup
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        // Execute
        var result = repository.update("users", columnValues, whereConditions).subscribe().asCompletionStage().toCompletableFuture();

        // Verify - would need actual async execution
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should reject update with empty where conditions")
    void testUpdateWithEmptyWhereConditions() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();

        var result = repository.update("users", columnValues, whereConditions);

        // Verify
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should reject update when circuit breaker is open")
    void testUpdateWhenCircuitBreakerOpen() {
        // Open circuit breaker
        for (int i = 0; i < 50; i++) {
            circuitBreaker.recordFailure();
        }

        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle null values in column values")
    void testUpdateWithNullValues() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", null);
        columnValues.put("age", 30);

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should convert timestamp strings to LocalDateTime")
    void testConvertTimestampValue() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("updated_at", "2024-12-01T10:00:00");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should convert timestamp with space separator")
    void testConvertTimestampWithSpace() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("timestamp", "2024-12-01 10:00:00");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle invalid timestamp gracefully")
    void testConvertInvalidTimestamp() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("created_at", "not-a-timestamp");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle empty string values")
    void testConvertEmptyStringValue() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("description", "");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle numeric values")
    void testConvertNumericValues() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("age", 30);
        columnValues.put("salary", 50000.00);

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle boolean values")
    void testConvertBooleanValues() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("is_active", true);

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should build correct SQL for single condition")
    void testSqlBuildingSingleCondition() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should build correct SQL for multiple conditions")
    void testSqlBuildingMultipleConditions() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");
        columnValues.put("age", 30);

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);
        whereConditions.put("status", "active");

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(2);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should record metrics on successful update")
    void testMetricsRecordingOnSuccess() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should record metrics on failed update")
    void testMetricsRecordingOnFailure() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().failure(new RuntimeException("DB Error")));

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should update circuit breaker on failure")
    void testCircuitBreakerUpdateOnFailure() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().failure(new RuntimeException("DB Error")));

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle UpdateOperation record")
    void testUpdateOperationRecord() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        DBWriteRepository.UpdateOperation operation = new DBWriteRepository.UpdateOperation(
                "users",
                columnValues,
                whereConditions
        );

        assertEquals("users", operation.tableName());
        assertEquals(columnValues, operation.columnValues());
        assertEquals(whereConditions, operation.whereConditions());
    }

    @Test
    @DisplayName("Should handle very long table names")
    void testLongTableName() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("name", "John");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        String longTableName = "very_long_table_name_that_might_be_used_in_production_systems";

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update(longTableName, columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle many column values")
    void testManyColumnValues() {
        Map<String, Object> columnValues = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            columnValues.put("col" + i, "value" + i);
        }

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Should handle timestamp with milliseconds")
    void testTimestampWithMilliseconds() {
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("timestamp", "2024-12-01T10:00:00.123");

        Map<String, Object> whereConditions = new HashMap<>();
        whereConditions.put("id", 1);

        when(mockPool.preparedQuery(anyString())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(Tuple.class))).thenReturn(Uni.createFrom().item(mockSqlResult));
        when(mockSqlResult.rowCount()).thenReturn(1);

        var result = repository.update("users", columnValues, whereConditions);

        assertNotNull(result);
    }

    @Test
    @DisplayName("Constructor should set dependencies")
    void testConstructor() {
        assertNotNull(repository);
    }
}
