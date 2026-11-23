/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;

public class RestSqlActionTest {

  private static final OpenSearchSecurityException DENIED =
      new OpenSearchSecurityException(
          "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN);

  @Before
  public void setUp() {
    Metrics.getInstance().clear();
    for (MetricName name :
        new MetricName[] {MetricName.FAILED_REQ_COUNT_CUS, MetricName.FAILED_REQ_COUNT_SYS}) {
      Metrics.getInstance().registerMetric(new NumericMetric<>(name.getName(), new BasicCounter()));
    }
  }

  @Test
  public void openSearchExceptionKeepsItsStatus() {
    assertEquals(RestStatus.FORBIDDEN, RestSqlAction.getRestStatus(DENIED));
  }

  @Test
  public void wrappedClientErrorKeepsItsStatus() {
    assertEquals(
        RestStatus.FORBIDDEN,
        RestSqlAction.getRestStatus(
            new IllegalStateException("Error occurred during join query run", DENIED)));
    assertEquals(
        RestStatus.NOT_FOUND,
        RestSqlAction.getRestStatus(
            new RuntimeException(
                "Error occurred while creating PIT.", new IndexNotFoundException("missing"))));
  }

  @Test
  public void wrappedTooManyRequestsAndServerErrorsKeepTheirStatus() {
    assertEquals(
        RestStatus.TOO_MANY_REQUESTS,
        RestSqlAction.getRestStatus(
            new IllegalStateException(
                new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS))));
    assertEquals(
        RestStatus.SERVICE_UNAVAILABLE,
        RestSqlAction.getRestStatus(
            new IllegalStateException(
                new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE))));
  }

  @Test
  public void illegalArgumentWrappingOpenSearchErrorUsesItsStatus() {
    assertEquals(
        RestStatus.SERVICE_UNAVAILABLE,
        RestSqlAction.getRestStatus(
            new IllegalArgumentException(
                new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE))));
    assertEquals(
        RestStatus.BAD_REQUEST,
        RestSqlAction.getRestStatus(new IllegalArgumentException("bad argument")));
  }

  @Test
  public void errorReportWrappingOpenSearchErrorUsesItsStatus() {
    assertEquals(
        RestStatus.TOO_MANY_REQUESTS,
        RestSqlAction.getRestStatus(
            ErrorReport.wrap(
                    new IllegalStateException(
                        new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS)))
                .build()));
  }

  @Test
  public void handleExceptionSendsWrappedTooManyRequestsWithMatchingBodyStatus() {
    RestChannel channel = mock(RestChannel.class);

    RestSqlAction.handleException(
        channel,
        new IllegalStateException(
            "Error occurred during join query run",
            new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS)));

    BytesRestResponse response = sentResponse(channel);
    assertEquals(RestStatus.TOO_MANY_REQUESTS, response.status());
    JSONObject body = new JSONObject(response.content().utf8ToString());
    assertEquals(429, body.getInt("status"));
    assertEquals("OpenSearchStatusException", body.getJSONObject("error").getString("type"));
    // This path counts by the response status range, so a 429 counts as a client failure.
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void handleExceptionSendsWrappedServiceUnavailableWithMatchingBodyStatus() {
    RestChannel channel = mock(RestChannel.class);

    RestSqlAction.handleException(
        channel,
        new IllegalStateException(
            new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE)));

    BytesRestResponse response = sentResponse(channel);
    assertEquals(RestStatus.SERVICE_UNAVAILABLE, response.status());
    assertEquals(503, new JSONObject(response.content().utf8ToString()).getInt("status"));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  private static BytesRestResponse sentResponse(RestChannel channel) {
    ArgumentCaptor<BytesRestResponse> captor = ArgumentCaptor.forClass(BytesRestResponse.class);
    verify(channel).sendResponse(captor.capture());
    return captor.getValue();
  }

  @Test
  public void handleExceptionReportsWrappedClientErrorAsClientError() {
    RestChannel channel = mock(RestChannel.class);

    RestSqlAction.handleException(
        channel, new IllegalStateException("Error occurred during join query run", DENIED));

    ArgumentCaptor<BytesRestResponse> captor = ArgumentCaptor.forClass(BytesRestResponse.class);
    verify(channel).sendResponse(captor.capture());
    assertEquals(RestStatus.FORBIDDEN, captor.getValue().status());
    JSONObject body = new JSONObject(captor.getValue().content().utf8ToString());
    assertEquals("OpenSearchSecurityException", body.getJSONObject("error").getString("type"));
    assertEquals(1L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  private static long failedRequestCount(MetricName name) {
    return (Long) Metrics.getInstance().getNumericalMetric(name).getValue();
  }

  @Test
  public void handleExceptionDoesNotLogClientErrorAsError() {
    List<LogEvent> events = new ArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("capture", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    Logger logger = (Logger) LogManager.getLogger(RestSqlAction.class);
    logger.addAppender(appender);
    try {
      RestSqlAction.handleException(mock(RestChannel.class), DENIED);
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }

    assertTrue(
        events.toString(),
        events.stream().noneMatch(event -> event.getLevel().isMoreSpecificThan(Level.WARN)));
  }
}
