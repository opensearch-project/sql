/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.scheduler;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.OK;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.spark.scheduler.model.OpenSearchRefreshIndexJobRequest;

/** A sample rest handler that supports schedule and deschedule job operation */
public class SampleExtensionRestHandler extends BaseRestHandler {
  private static final Logger LOG = LogManager.getLogger(SampleExtensionRestHandler.class);

  public static final String WATCH_INDEX_URI = "/_plugins/scheduler_sample/watch";
  private final OpenSearchAsyncQueryScheduler scheduler;

  public SampleExtensionRestHandler(OpenSearchAsyncQueryScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public String getName() {
    return "Sample JobScheduler extension handler";
  }

  @Override
  public List<Route> routes() {
    return Collections.unmodifiableList(
        Arrays.asList(
            new Route(RestRequest.Method.POST, WATCH_INDEX_URI),
            new Route(RestRequest.Method.DELETE, WATCH_INDEX_URI)));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    switch (request.method()) {
      case POST:
        return handlePostRequest(request);
      case DELETE:
        return handleDeleteRequest(request);
      default:
        return restChannel -> {
          restChannel.sendResponse(
              new BytesRestResponse(
                  RestStatus.METHOD_NOT_ALLOWED, request.method() + " is not allowed."));
        };
    }
  }

  private RestChannelConsumer handlePostRequest(RestRequest request) {
    String jobName = request.param("job_name");
    String interval = request.param("interval");

    if (jobName == null || interval == null) {
      return restChannel ->
          restChannel.sendResponse(
              new BytesRestResponse(
                  BAD_REQUEST, "Missing required parameters: id, index, job_name, interval"));
    }

    return restChannel -> {
      try {
        Instant now = Instant.now();
        OpenSearchRefreshIndexJobRequest jobRequest =
            OpenSearchRefreshIndexJobRequest.builder()
                .jobName(jobName)
                .schedule(
                    new IntervalSchedule(
                        now,
                        Integer.parseInt(interval),
                        ChronoUnit
                            .MINUTES)) // Assuming ScheduleParser can parse the interval directly
                .enabled(true)
                .enabledTime(now)
                .lastUpdateTime(now)
                .lockDurationSeconds(1L)
                .build();
        scheduler.scheduleJob(jobRequest);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject().field("message", "Scheduled job with name " + jobName).endObject();
        restChannel.sendResponse(new BytesRestResponse(OK, builder));
      } catch (Exception e) {
        LOG.error("Failed to schedule job", e);
        restChannel.sendResponse(
            new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
      }
    };
  }

  private RestChannelConsumer handleDeleteRequest(RestRequest request) {
    String jobName = request.param("job_name");
    if (jobName == null) {
      return restChannel ->
          restChannel.sendResponse(
              new BytesRestResponse(BAD_REQUEST, "Must specify jobName parameter"));
    }

    return restChannel -> {
      try {
        scheduler.removeJob(jobName);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder
            .startObject()
            .field("message", "Remove scheduled job with name " + jobName)
            .endObject();
        restChannel.sendResponse(new BytesRestResponse(OK, builder));
      } catch (Exception e) {
        LOG.error("Failed to schedule job", e);
        restChannel.sendResponse(
            new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
      }
    };
  }
}
