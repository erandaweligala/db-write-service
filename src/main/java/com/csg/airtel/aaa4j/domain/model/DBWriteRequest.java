package com.csg.airtel.aaa4j.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)  // ADD THIS
public class DBWriteRequest {
    private String eventType;
    private String timestamp;
    private String userName;
    private Map<String, Object> columnValues;
    private Map<String, Object> whereConditions;
    private String tableName;
    private List<DBWriteRequest> relatedWrites;
}