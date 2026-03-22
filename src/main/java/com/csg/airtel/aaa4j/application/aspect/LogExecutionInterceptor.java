package com.csg.airtel.aaa4j.application.aspect;

import com.csg.airtel.aaa4j.application.common.LoggingUtil;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * Interceptor for logging execution time, parameters, and results of service methods.
 */
@LogDomainService
@Interceptor
public class LogExecutionInterceptor {

    private static final Logger log = Logger.getLogger(LogExecutionInterceptor.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    @AroundInvoke
    public Object logMethod(InvocationContext ctx) throws Exception {

        if (!log.isDebugEnabled()) {
            return ctx.proceed();
        }

        String className = ctx.getTarget().getClass().getSimpleName();
        String methodName = ctx.getMethod().getName();
        Object[] params = ctx.getParameters();

        // Get traceId and optional MDC values
        String traceId = MDC.get("traceId") != null ? MDC.get("traceId").toString() : "-";
        String userName = MDC.get("userName") != null ? MDC.get("userName").toString() : "-";
        String sessionId = MDC.get("sessionId") != null ? MDC.get("sessionId").toString() : "-";

        long startTime = System.nanoTime();
        String startTimeStr = ZonedDateTime.now().format(TIME_FORMATTER);

        // IN log
        LoggingUtil.logDebug(log, methodName, "[%s][%s][%s][user=%s][session=%s] Invoke method args=%s",
                startTimeStr, traceId, className, userName, sessionId, Arrays.toString(params));
        try {
            Object result = ctx.proceed();
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            String endTimeStr = ZonedDateTime.now().format(TIME_FORMATTER);

            // OUT log
            LoggingUtil.logDebug(log, methodName, "[%s][%s][%s][user=%s][session=%s] Completed result=%s [%d ms]",
                    endTimeStr, traceId, className, userName, sessionId, result, durationMs);

            return result;
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            String errorTimeStr = ZonedDateTime.now().format(TIME_FORMATTER);

            LoggingUtil.logError(log, methodName, e,
                    "[%s][%s][%s][user=%s][session=%s] ERROR after %d ms: %s",
                    errorTimeStr, traceId, className, userName, sessionId, durationMs, e.getMessage());
            throw e;
        }
    }
}