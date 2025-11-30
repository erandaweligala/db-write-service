package com.csg.airtel.aaa4j.external.repository;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;
import java.util.*;

@ApplicationScoped
public class DBWriteRepository {
    private static final Logger log = Logger.getLogger(DBWriteRepository.class);

    final Pool client;

    @Inject
    public DBWriteRepository(Pool client) {
        this.client = client;
    }
    /**
     * Reactive Generic Update Method
     */

    public Uni<Integer> update(String tableName,
                               Map<String, Object> columnValues,
                               Map<String, Object> whereConditions) {

        if (whereConditions.isEmpty()) {
            log.warn("Update operation rejected: WHERE conditions are required");
            return Uni.createFrom().failure(new IllegalArgumentException("WHERE conditions required"));
        }

        if (log.isDebugEnabled()) {
            log.debugf("Update: table=%s, cols=%d, conditions=%d",
                    tableName, columnValues.size(), whereConditions.size());
        }

        return executeUpdate(tableName, columnValues, whereConditions)
                .onItem().invoke(rowCount -> {
                    if (log.isDebugEnabled()) {
                        log.debugf("Updated %s: %d rows", tableName, rowCount);
                    }
                })
                .onFailure().invoke(throwable ->
                        log.errorf(throwable, "Update failed: %s", tableName));
    }


    private Uni<Integer> executeUpdate(String tableName,
                                       Map<String, Object> columnValues,
                                       Map<String, Object> whereConditions) {

        int setCount = columnValues.size();
        int whereCount = whereConditions.size();
        int totalParams = setCount + whereCount;

        StringBuilder sql = new StringBuilder(64 + tableName.length() + (totalParams * 10));
        sql.append("UPDATE ").append(tableName).append(" SET ");

        Object[] values = new Object[totalParams];
        int idx = 0;

        Iterator<Map.Entry<String, Object>> setIter = columnValues.entrySet().iterator();
        while (setIter.hasNext()) {
            Map.Entry<String, Object> entry = setIter.next();
            sql.append(entry.getKey()).append(" = ?");
            values[idx++] = convertValue(entry.getValue());
            if (setIter.hasNext()) sql.append(", ");
        }

        sql.append(" WHERE ");
        Iterator<Map.Entry<String, Object>> whereIter = whereConditions.entrySet().iterator();
        while (whereIter.hasNext()) {
            Map.Entry<String, Object> entry = whereIter.next();
            sql.append(entry.getKey()).append(" = ?");
            values[idx++] = entry.getValue();
            if (whereIter.hasNext()) sql.append(" AND ");
        }
        String sqlStr = sql.toString();
        if (log.isDebugEnabled()) {
            log.debugf("SQL: %s", sqlStr);
        }

        Tuple tuple = Tuple.from(values);
        return client.preparedQuery(sqlStr)
                .execute(tuple)
                .map(SqlResult::rowCount);
    }

    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }

        if(value instanceof String strValue && strValue.matches("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}.*")) {

                try {

                    String isoFormat = strValue.replace(' ', 'T');
                    return LocalDateTime.parse(isoFormat,
                            java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                } catch (Exception e) {
                    log.warnf("Failed to parse timestamp string: %s", strValue);
                    return value;
                }
            }

        return value;
    }


}
