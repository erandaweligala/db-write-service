package com.csg.airtel.aaa4j.domain.model;

import java.util.Map;

public class DBWriteRequest {
    private String eventType;
    private String timestamp;
    private String userName;
    private Map<String, Object> columnValues;
    private Map<String, Object> whereConditions;
    private String tableName;

    public DBWriteRequest(String tableName, Map<String, Object> whereConditions, Map<String, Object> columnValues, String userName, String timestamp, String eventType) {
        this.tableName = tableName;
        this.whereConditions = whereConditions;
        this.columnValues = columnValues;
        this.userName = userName;
        this.timestamp = timestamp;
        this.eventType = eventType;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getColumnValues() {
        return columnValues;
    }

    public void setColumnValues(Map<String, Object> columnValues) {
        this.columnValues = columnValues;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Object> getWhereConditions() {
        return whereConditions;
    }

    public void setWhereConditions(Map<String, Object> whereConditions) {
        this.whereConditions = whereConditions;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }
}
