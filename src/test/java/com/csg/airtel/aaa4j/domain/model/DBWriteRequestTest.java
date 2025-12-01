package com.csg.airtel.aaa4j.domain.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DBWriteRequest Tests")
class DBWriteRequestTest {

    private DBWriteRequest dbWriteRequest;
    private Map<String, Object> columnValues;
    private Map<String, Object> whereConditions;

    @BeforeEach
    void setUp() {
        columnValues = new HashMap<>();
        columnValues.put("name", "John");
        columnValues.put("age", 30);

        whereConditions = new HashMap<>();
        whereConditions.put("id", 123);
        whereConditions.put("status", "active");

        dbWriteRequest = new DBWriteRequest(
                "users",
                whereConditions,
                columnValues,
                "testUser",
                "2024-12-01T10:00:00",
                "UPDATE_EVENT"
        );
    }

    @Test
    @DisplayName("Should create DBWriteRequest with all parameters")
    void testConstructor() {
        assertNotNull(dbWriteRequest);
        assertEquals("users", dbWriteRequest.getTableName());
        assertEquals("testUser", dbWriteRequest.getUserName());
        assertEquals("2024-12-01T10:00:00", dbWriteRequest.getTimestamp());
        assertEquals("UPDATE_EVENT", dbWriteRequest.getEventType());
        assertEquals(2, dbWriteRequest.getColumnValues().size());
        assertEquals(2, dbWriteRequest.getWhereConditions().size());
    }

    @Test
    @DisplayName("Should get and set table name")
    void testTableName() {
        dbWriteRequest.setTableName("products");
        assertEquals("products", dbWriteRequest.getTableName());
    }

    @Test
    @DisplayName("Should get and set column values")
    void testColumnValues() {
        Map<String, Object> newValues = new HashMap<>();
        newValues.put("price", 99.99);
        newValues.put("quantity", 100);

        dbWriteRequest.setColumnValues(newValues);
        assertEquals(newValues, dbWriteRequest.getColumnValues());
        assertEquals(2, dbWriteRequest.getColumnValues().size());
    }

    @Test
    @DisplayName("Should get and set where conditions")
    void testWhereConditions() {
        Map<String, Object> newConditions = new HashMap<>();
        newConditions.put("product_id", 456);

        dbWriteRequest.setWhereConditions(newConditions);
        assertEquals(newConditions, dbWriteRequest.getWhereConditions());
        assertEquals(1, dbWriteRequest.getWhereConditions().size());
    }

    @Test
    @DisplayName("Should get and set user name")
    void testUserName() {
        dbWriteRequest.setUserName("adminUser");
        assertEquals("adminUser", dbWriteRequest.getUserName());
    }

    @Test
    @DisplayName("Should get and set timestamp")
    void testTimestamp() {
        String newTimestamp = "2024-12-02T15:30:00";
        dbWriteRequest.setTimestamp(newTimestamp);
        assertEquals(newTimestamp, dbWriteRequest.getTimestamp());
    }

    @Test
    @DisplayName("Should get and set event type")
    void testEventType() {
        dbWriteRequest.setEventType("DELETE_EVENT");
        assertEquals("DELETE_EVENT", dbWriteRequest.getEventType());
    }

    @Test
    @DisplayName("Should handle null column values")
    void testNullColumnValues() {
        dbWriteRequest.setColumnValues(null);
        assertNull(dbWriteRequest.getColumnValues());
    }

    @Test
    @DisplayName("Should handle null where conditions")
    void testNullWhereConditions() {
        dbWriteRequest.setWhereConditions(null);
        assertNull(dbWriteRequest.getWhereConditions());
    }

    @Test
    @DisplayName("Should handle empty maps")
    void testEmptyMaps() {
        Map<String, Object> emptyMap = new HashMap<>();
        dbWriteRequest.setColumnValues(emptyMap);
        dbWriteRequest.setWhereConditions(emptyMap);

        assertTrue(dbWriteRequest.getColumnValues().isEmpty());
        assertTrue(dbWriteRequest.getWhereConditions().isEmpty());
    }

    @Test
    @DisplayName("Should handle various data types in column values")
    void testVariousDataTypes() {
        Map<String, Object> mixedValues = new HashMap<>();
        mixedValues.put("stringValue", "test");
        mixedValues.put("intValue", 42);
        mixedValues.put("doubleValue", 3.14);
        mixedValues.put("boolValue", true);
        mixedValues.put("nullValue", null);

        dbWriteRequest.setColumnValues(mixedValues);
        assertEquals(5, dbWriteRequest.getColumnValues().size());
        assertEquals("test", dbWriteRequest.getColumnValues().get("stringValue"));
        assertEquals(42, dbWriteRequest.getColumnValues().get("intValue"));
        assertEquals(3.14, dbWriteRequest.getColumnValues().get("doubleValue"));
        assertEquals(true, dbWriteRequest.getColumnValues().get("boolValue"));
        assertNull(dbWriteRequest.getColumnValues().get("nullValue"));
    }

    @Test
    @DisplayName("Should handle empty event type")
    void testEmptyEventType() {
        dbWriteRequest.setEventType("");
        assertEquals("", dbWriteRequest.getEventType());
    }

    @Test
    @DisplayName("Should handle null event type")
    void testNullEventType() {
        dbWriteRequest.setEventType(null);
        assertNull(dbWriteRequest.getEventType());
    }

    @Test
    @DisplayName("Should handle multiple sequential updates")
    void testSequentialUpdates() {
        dbWriteRequest.setTableName("users");
        dbWriteRequest.setUserName("user1");
        dbWriteRequest.setEventType("UPDATE_EVENT");
        dbWriteRequest.setTimestamp("2024-12-01T10:00:00");

        assertEquals("users", dbWriteRequest.getTableName());
        assertEquals("user1", dbWriteRequest.getUserName());
        assertEquals("UPDATE_EVENT", dbWriteRequest.getEventType());
        assertEquals("2024-12-01T10:00:00", dbWriteRequest.getTimestamp());
    }
}
