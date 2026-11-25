/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.SearchContextMissingException;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;
import org.opensearch.sql.legacy.util.LogCapture;
import org.opensearch.transport.RemoteTransportException;

public class OpenSearchFailuresTest {

  private static final OpenSearchSecurityException DENIED =
      new OpenSearchSecurityException(
          "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN);

  private static final QueryShardException BAD_VALUE =
      new QueryShardException(
          new Index("logs", "_na_"), "failed to create query: For input string: \"abc\"", null);

  private static final OpenSearchStatusException BUSY =
      new OpenSearchStatusException(
          "too many open point in time contexts", RestStatus.TOO_MANY_REQUESTS);

  private static final OpenSearchStatusException UNAVAILABLE =
      new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE);

  private static final SearchContextMissingException LOST_PIT =
      new SearchContextMissingException(new ShardSearchContextId("session", 1L));

  @Before
  public void setUp() {
    Metrics.getInstance().clear();
    for (MetricName name :
        new MetricName[] {MetricName.FAILED_REQ_COUNT_CUS, MetricName.FAILED_REQ_COUNT_SYS}) {
      Metrics.getInstance().registerMetric(new NumericMetric<>(name.getName(), new BasicCounter()));
    }
  }

  @Test
  public void findsOpenSearchCauseOfAnyStatus() {
    assertSame(
        BUSY,
        OpenSearchFailures.findOpenSearchCause(
                new IllegalStateException(new RuntimeException(BUSY)))
            .orElseThrow());
    assertSame(
        UNAVAILABLE,
        OpenSearchFailures.findOpenSearchCause(new RuntimeException(UNAVAILABLE)).orElseThrow());
    assertSame(DENIED, OpenSearchFailures.findOpenSearchCause(DENIED).orElseThrow());
  }

  @Test
  public void findOpenSearchCauseUsesTheFirstOpenSearchExceptionInTheChain() {
    OpenSearchException outer = new OpenSearchException("failed", DENIED);

    assertSame(
        outer,
        OpenSearchFailures.findOpenSearchCause(new IllegalStateException(outer)).orElseThrow());
    assertEquals(RestStatus.INTERNAL_SERVER_ERROR, outer.status());
  }

  @Test
  public void findOpenSearchCauseIsEmptyWithoutOpenSearchException() {
    assertEquals(
        Optional.empty(),
        OpenSearchFailures.findOpenSearchCause(
            new IllegalStateException(new IllegalArgumentException("bad argument"))));
  }

  @Test
  public void findsClientErrorOnTopAndUnderWrappers() {
    Exception wrapped =
        new IllegalStateException(
            "Error occurred during join query run",
            new RuntimeException(new ExecutionException(DENIED)));

    assertSame(DENIED, OpenSearchFailures.findClientErrorCause(DENIED).orElseThrow());
    assertSame(DENIED, OpenSearchFailures.findClientErrorCause(wrapped).orElseThrow());
  }

  @Test
  public void everyClientStatusIsClientErrorIncludingTooManyRequests() {
    IndexNotFoundException notFound = new IndexNotFoundException("logs");
    OpenSearchStatusException badRequest =
        new OpenSearchStatusException("bad request", RestStatus.BAD_REQUEST);

    assertSame(
        notFound,
        OpenSearchFailures.findClientErrorCause(new RuntimeException(notFound)).orElseThrow());
    assertSame(
        badRequest,
        OpenSearchFailures.findClientErrorCause(new SQLException(badRequest)).orElseThrow());
    assertSame(
        BUSY, OpenSearchFailures.findClientErrorCause(new RuntimeException(BUSY)).orElseThrow());
  }

  @Test
  public void serverErrorAndNonOpenSearchFailureAreNotClientErrors() {
    assertEquals(
        Optional.empty(),
        OpenSearchFailures.findClientErrorCause(new RuntimeException(UNAVAILABLE)));
    assertEquals(
        Optional.empty(),
        OpenSearchFailures.findClientErrorCause(new IllegalArgumentException("bad argument")));
  }

  @Test
  public void clientErrorUsesTheFirstOpenSearchExceptionInTheChain() {
    // A wrapper whose own status is 500 decides, as it does for the error body.
    OpenSearchException outer = new OpenSearchException("failed", DENIED);

    assertEquals(Optional.empty(), OpenSearchFailures.findClientErrorCause(outer));
  }

  @Test
  public void deniedAndAnyNotFoundAreDeniedOrMissing() {
    assertTrue(
        OpenSearchFailures.isDeniedOrMissing(
            new OpenSearchSecurityException("unauthenticated", RestStatus.UNAUTHORIZED)));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(DENIED));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(new IllegalStateException(DENIED)));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(new IndexNotFoundException("logs")));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(LOST_PIT));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(new IllegalStateException(LOST_PIT)));
    assertTrue(
        OpenSearchFailures.isDeniedOrMissing(new ResourceNotFoundException("model [m] missing")));
  }

  @Test
  public void transportWrapperAroundDeniedOrMissingIsDeniedOrMissing() {
    RemoteTransportException missingIndex =
        new RemoteTransportException(
            "[node][indices:data/read/search]", new IndexNotFoundException("logs"));
    RemoteTransportException lostPit =
        new RemoteTransportException("[node][indices:data/read/search]", LOST_PIT);
    RemoteTransportException denied =
        new RemoteTransportException("[node][indices:data/read/search]", DENIED);

    assertEquals(RestStatus.NOT_FOUND, missingIndex.status());
    assertTrue(OpenSearchFailures.isDeniedOrMissing(missingIndex));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(new IllegalStateException(missingIndex)));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(lostPit));
    assertTrue(OpenSearchFailures.isDeniedOrMissing(denied));
  }

  @Test
  public void otherFailuresAreNotDeniedOrMissing() {
    assertFalse(OpenSearchFailures.isDeniedOrMissing(new RuntimeException(BAD_VALUE)));
    assertFalse(OpenSearchFailures.isDeniedOrMissing(BUSY));
    assertFalse(OpenSearchFailures.isDeniedOrMissing(UNAVAILABLE));
    assertFalse(OpenSearchFailures.isDeniedOrMissing(new OpenSearchException("failed", LOST_PIT)));
    assertFalse(OpenSearchFailures.isDeniedOrMissing(new IllegalArgumentException("bad")));
  }

  @Test
  public void logFailureLogsDeniedAtWarnWithoutStackTrace() throws Exception {
    LogEvent event = logFailure(Level.ERROR, new IllegalStateException(DENIED));

    assertEquals(Level.WARN, event.getLevel());
    assertNull(event.getThrown());
    assertMessageContains(event, "no permissions for [indices:data/read/search]");
  }

  @Test
  public void logFailureLogsBadRequestAtWarnWithoutStackTrace() throws Exception {
    LogEvent event = logFailure(Level.ERROR, new IllegalStateException(BAD_VALUE));

    assertEquals(Level.WARN, event.getLevel());
    assertNull(event.getThrown());
    assertMessageContains(event, "For input string");
  }

  @Test
  public void logFailureLogsTooManyRequestsAtWarnWithoutStackTrace() throws Exception {
    LogEvent event = logFailure(Level.ERROR, new IllegalStateException(new RuntimeException(BUSY)));

    assertEquals(Level.WARN, event.getLevel());
    assertNull(event.getThrown());
    assertMessageContains(event, "too many open point in time contexts");
  }

  @Test
  public void logFailureLogsLostPointInTimeAtWarnWithoutStackTrace() throws Exception {
    LogEvent event = logFailure(Level.ERROR, new IllegalStateException(LOST_PIT));

    assertEquals(Level.WARN, event.getLevel());
    assertNull(event.getThrown());
    assertMessageContains(event, "No search context found");
  }

  @Test
  public void logFailureLogsMissingIndexFromRemoteNodeAtWarn() throws Exception {
    LogEvent event =
        logFailure(
            Level.ERROR,
            new IllegalStateException(
                new RemoteTransportException(
                    "[node][indices:data/read/search]", new IndexNotFoundException("logs"))));

    assertEquals(Level.WARN, event.getLevel());
    assertNull(event.getThrown());
  }

  @Test
  public void logFailureKeepsALowerLevelForClientError() throws Exception {
    LogEvent event = logFailure(Level.INFO, DENIED);

    assertEquals(Level.INFO, event.getLevel());
    assertNull(event.getThrown());
  }

  @Test
  public void logFailureLogsServerErrorAtGivenLevelWithStackTrace() throws Exception {
    Exception failure = new IllegalStateException(UNAVAILABLE);

    LogEvent event = logFailure(Level.ERROR, failure);

    assertEquals(Level.ERROR, event.getLevel());
    assertSame(failure, event.getThrown());
  }

  @Test
  public void logFailureLogsNonOpenSearchFailureAtGivenLevelWithStackTrace() throws Exception {
    Exception failure = new IllegalStateException("Memory circuit is broken");

    LogEvent event = logFailure(Level.WARN, failure);

    assertEquals(Level.WARN, event.getLevel());
    assertSame(failure, event.getThrown());
  }

  @Test
  public void countFailureCountsClientStatusesAsClientFailures() {
    OpenSearchFailures.countFailure(400);
    OpenSearchFailures.countFailure(403);
    OpenSearchFailures.countFailure(429);

    assertEquals(3L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void countFailureCountsServerStatusesAsSystemFailures() {
    OpenSearchFailures.countFailure(500);
    OpenSearchFailures.countFailure(503);
    OpenSearchFailures.countFailure(507);

    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(3L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  @Test
  public void countFailureIgnoresOtherStatuses() {
    OpenSearchFailures.countFailure(200);
    OpenSearchFailures.countFailure(302);

    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_CUS));
    assertEquals(0L, failedRequestCount(MetricName.FAILED_REQ_COUNT_SYS));
  }

  private static LogEvent logFailure(Level level, Exception failure) throws Exception {
    List<LogEvent> events =
        LogCapture.capture(
            OpenSearchFailuresTest.class,
            () ->
                OpenSearchFailures.logFailure(
                    LogManager.getLogger(OpenSearchFailuresTest.class),
                    level,
                    "Failed during join query run.",
                    failure));
    assertEquals(events.toString(), 1, events.size());
    return events.get(0);
  }

  private static void assertMessageContains(LogEvent event, String expected) {
    String message = event.getMessage().getFormattedMessage();
    assertTrue(message, message.contains(expected));
  }

  private static long failedRequestCount(MetricName name) {
    return (Long) Metrics.getInstance().getNumericalMetric(name).getValue();
  }
}
