package com.csg.airtel.aaa4j.domain.service;

import com.csg.airtel.aaa4j.domain.model.DBWriteRequest;
import com.csg.airtel.aaa4j.external.repository.DBWriteRepository;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DBWriteServiceTest {

    @Mock
    DBWriteRepository dbWriteRepository;

    @Mock
    DBOperationsService dbOperationsService;

    @Mock
    Pool pool;

    @Mock
    SqlConnection sqlConnection;

    DBWriteService service;

    @BeforeEach
    void setUp() {
        service = new DBWriteService(dbWriteRepository, dbOperationsService, pool);
    }

    // -----------------------------------------------------------------------
    // processEvent — null / blank guards
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("processEvent: null request returns void without touching repository")
    void processEvent_nullRequest_returnsVoid() {
        Uni<Void> result = service.processEvent(null);

        assertDoesNotThrow(() -> result.await().indefinitely());
        verifyNoInteractions(dbWriteRepository, pool);
    }

    @Test
    @DisplayName("processEvent: blank eventType skips all DB work")
    void processEvent_blankEventType_skips() {
        DBWriteRequest request = requestWith("  ", "my_table", null, null, null);

        Uni<Void> result = service.processEvent(request);

        assertDoesNotThrow(() -> result.await().indefinitely());
        verifyNoInteractions(dbWriteRepository, pool);
    }

    // -----------------------------------------------------------------------
    // processEvent — single writes (no transaction)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("processEvent: CREATE routes to executeInsert without transaction")
    void processEvent_create_callsInsert() {
        Map<String, Object> cols = Map.of("name", "Alice");
        DBWriteRequest request = requestWith("CREATE", "users", cols, null, null);

        when(dbWriteRepository.executeInsert("users", cols)).thenReturn(Uni.createFrom().item(1));

        service.processEvent(request).await().indefinitely();

        verify(dbWriteRepository).executeInsert("users", cols);
        verifyNoInteractions(pool);
    }

    @Test
    @DisplayName("processEvent: UPDATE routes to update without transaction")
    void processEvent_update_callsUpdate() {
        Map<String, Object> cols  = Map.of("status", "active");
        Map<String, Object> where = Map.of("id", 42);
        DBWriteRequest request = requestWith("UPDATE", "users", cols, where, null);

        when(dbWriteRepository.update("users", cols, where)).thenReturn(Uni.createFrom().item(1));

        service.processEvent(request).await().indefinitely();

        verify(dbWriteRepository).update("users", cols, where);
        verifyNoInteractions(pool);
    }

    @Test
    @DisplayName("processEvent: DELETE routes to executeDelete without transaction")
    void processEvent_delete_callsDelete() {
        Map<String, Object> where = Map.of("id", 7);
        DBWriteRequest request = requestWith("DELETE", "users", null, where, null);

        when(dbWriteRepository.executeDelete("users", where)).thenReturn(Uni.createFrom().item(1));

        service.processEvent(request).await().indefinitely();

        verify(dbWriteRepository).executeDelete("users", where);
        verifyNoInteractions(pool);
    }

    @Test
    @DisplayName("processEvent: unknown eventType logs warning and returns void")
    void processEvent_unknownEventType_skips() {
        DBWriteRequest request = requestWith("EXPLODE", "users", null, null, null);

        Uni<Void> result = service.processEvent(request);

        assertDoesNotThrow(() -> result.await().indefinitely());
        verifyNoInteractions(dbWriteRepository, pool);
    }

    // -----------------------------------------------------------------------
    // processEvent — with relatedWrites (transaction)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("processEvent: request with relatedWrites wraps everything in a transaction")
    void processEvent_withRelatedWrites_usesTransaction() {
        Map<String, Object> cols  = Map.of("x", 1);
        Map<String, Object> where = Map.of("id", 1);

        DBWriteRequest related = requestWith("DELETE", "audit", null, where, null);
        DBWriteRequest request  = requestWith("UPDATE", "users", cols, where, List.of(related));

        // Capture the transaction function and execute it with our mock SqlConnection
        when(pool.withTransaction(any())).thenAnswer(inv -> {
            Function<SqlConnection, Uni<Void>> txFn = inv.getArgument(0);
            return txFn.apply(sqlConnection);
        });

        when(dbWriteRepository.update(sqlConnection, "users", cols, where))
                .thenReturn(Uni.createFrom().item(1));
        when(dbWriteRepository.executeDelete(sqlConnection, "audit", where))
                .thenReturn(Uni.createFrom().item(1));

        service.processEvent(request).await().indefinitely();

        verify(pool).withTransaction(any());
        verify(dbWriteRepository).update(sqlConnection, "users", cols, where);
        verify(dbWriteRepository).executeDelete(sqlConnection, "audit", where);
    }

    @Test
    @DisplayName("processEvent: relatedWrites with CREATE uses transactional insert")
    void processEvent_withRelatedCreate_usesTransactionalInsert() {
        Map<String, Object> cols      = Map.of("col", "val");
        Map<String, Object> where     = Map.of("id", 99);
        Map<String, Object> relCols   = Map.of("ref", "data");

        DBWriteRequest related = requestWith("CREATE", "log_table", relCols, null, null);
        DBWriteRequest request  = requestWith("UPDATE", "main_table", cols, where, List.of(related));

        when(pool.withTransaction(any())).thenAnswer(inv -> {
            Function<SqlConnection, Uni<Void>> txFn = inv.getArgument(0);
            return txFn.apply(sqlConnection);
        });
        when(dbWriteRepository.update(sqlConnection, "main_table", cols, where))
                .thenReturn(Uni.createFrom().item(1));
        when(dbWriteRepository.executeInsert(sqlConnection, "log_table", relCols))
                .thenReturn(Uni.createFrom().item(1));

        service.processEvent(request).await().indefinitely();

        verify(dbWriteRepository).update(sqlConnection, "main_table", cols, where);
        verify(dbWriteRepository).executeInsert(sqlConnection, "log_table", relCols);
    }

    // -----------------------------------------------------------------------
    // processDbWriteRequest — legacy UPDATE_EVENT path
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("processDbWriteRequest: UPDATE_EVENT calls update then processRelatedWrites")
    void processDbWriteRequest_updateEvent_callsUpdate() {
        Map<String, Object> cols  = Map.of("status", "done");
        Map<String, Object> where = Map.of("id", 3);
        DBWriteRequest request = requestWith("UPDATE_EVENT", "tasks", cols, where, null);

        when(dbWriteRepository.update("tasks", cols, where)).thenReturn(Uni.createFrom().item(1));

        service.processDbWriteRequest(request).await().indefinitely();

        verify(dbWriteRepository).update("tasks", cols, where);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static DBWriteRequest requestWith(String eventType,
                                              String tableName,
                                              Map<String, Object> columnValues,
                                              Map<String, Object> whereConditions,
                                              List<DBWriteRequest> relatedWrites) {
        DBWriteRequest r = mock(DBWriteRequest.class);
        lenient().when(r.getEventType()).thenReturn(eventType);
        lenient().when(r.getTableName()).thenReturn(tableName);
        lenient().when(r.getColumnValues()).thenReturn(columnValues);
        lenient().when(r.getWhereConditions()).thenReturn(whereConditions);
        lenient().when(r.getRelatedWrites()).thenReturn(relatedWrites);
        lenient().when(r.getUserName()).thenReturn("test-user");
        return r;
    }
}