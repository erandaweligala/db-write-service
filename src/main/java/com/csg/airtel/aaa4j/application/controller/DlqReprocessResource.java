package com.csg.airtel.aaa4j.application.controller;

import com.csg.airtel.aaa4j.infrastructure.DlqMetrics;
import com.csg.airtel.aaa4j.infrastructure.dlq.DlqReprocessor;
import com.csg.airtel.aaa4j.infrastructure.dlq.ReprocessSummary;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operator surface for replaying dead-letter topics. Companion to {@code /api/monitoring/dlq},
 * which only reports how many records were dead-lettered — this endpoint actually recovers them.
 *
 * <ul>
 *   <li>{@code GET  /api/dlq/topics}            — configured DLT topics + current reprocess metrics.</li>
 *   <li>{@code POST /api/dlq/reprocess/{topic}} — drain one DLT (optional {@code ?max=N}).</li>
 *   <li>{@code POST /api/dlq/reprocess-all}     — drain every configured DLT (optional {@code ?max=N}).</li>
 * </ul>
 *
 * Runs are blocking (a bounded Kafka drain) and serialized by the reprocessor's lock, so the
 * methods are dispatched to worker threads via {@link Blocking}.
 */
@Path("/api/dlq")
public class DlqReprocessResource {

    private static final Logger log = Logger.getLogger(DlqReprocessResource.class);

    @Inject
    DlqReprocessor reprocessor;

    @Inject
    DlqMetrics dlqMetrics;

    @GET
    @Path("/topics")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> topics() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled", reprocessor.isEnabled());
        out.put("topics", reprocessor.configuredTopics());
        out.put("reprocessTotal", dlqMetrics.getReprocessTotal());
        return out;
    }

    @POST
    @Path("/reprocess/{topic}")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public Response reprocess(@PathParam("topic") String topic,
                              @QueryParam("max") @DefaultValue("0") int max) {
        log.infof("DLQ reprocess requested via API: topic=%s max=%d", topic, max);
        ReprocessSummary summary = reprocessor.reprocessTopic(topic, max);
        return Response.status(statusCodeFor(summary.status())).entity(summary).build();
    }

    @POST
    @Path("/reprocess-all")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public Map<String, ReprocessSummary> reprocessAll(@QueryParam("max") @DefaultValue("0") int max) {
        log.infof("DLQ reprocess-all requested via API: max=%d", max);
        return reprocessor.reprocessAll(max);
    }

    private static Response.Status statusCodeFor(ReprocessSummary.Status status) {
        return switch (status) {
            case COMPLETED -> Response.Status.OK;
            case DISABLED -> Response.Status.SERVICE_UNAVAILABLE;
            case BUSY -> Response.Status.CONFLICT;
            case UNKNOWN_TOPIC -> Response.Status.NOT_FOUND;
            case NO_PARTITIONS -> Response.Status.OK;
            case ERROR -> Response.Status.INTERNAL_SERVER_ERROR;
        };
    }
}
