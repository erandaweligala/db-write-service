package com.csg.airtel.aaa4j.application.aspect;

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
        if(log.isDebugEnabled()) {
            log.debugf("[%s][%s][%s][%s][user=%s][session=%s] Invoke method args=%s",
                    startTimeStr, traceId, className, methodName, userName, sessionId, Arrays.toString(params));
        }
        try {
            Object result = ctx.proceed();
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            String endTimeStr = ZonedDateTime.now().format(TIME_FORMATTER);

            // OUT log
            log.debugf("[%s][%s][%s][%s][user=%s][session=%s] Completed result=%s [%d ms]",
                    endTimeStr, traceId, className, methodName, userName, sessionId, result, durationMs);

            return result;
        } catch (Exception e) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            String errorTimeStr = ZonedDateTime.now().format(TIME_FORMATTER);

            log.errorf(e, "[%s][%s][%s][%s][user=%s][session=%s] ERROR after %d ms: %s",
                    errorTimeStr, traceId, className, methodName, userName, sessionId, durationMs, e.getMessage());
            throw e;
        }
    }
}