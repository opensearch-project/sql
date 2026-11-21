/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.legacy.executor.format.ErrorMessage;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.executor.format.OpenSearchErrorMessage;

public class ErrorMessageFactoryTest {

  private final Throwable nonOpenSearchThrowable = new Throwable();
  private final Throwable openSearchThrowable = new OpenSearchException(nonOpenSearchThrowable);

  @Test
  public void openSearchExceptionShouldCreateEsErrorMessage() {
    Exception exception = new OpenSearchException(nonOpenSearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    Assert.assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void nonOpenSearchExceptionShouldCreateGenericErrorMessage() {
    Exception exception = new Exception(nonOpenSearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    Assert.assertFalse(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void nonOpenSearchExceptionWithWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = (Exception) openSearchThrowable;
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    Assert.assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void errorReportShouldRenderUnderlyingCauseType() {
    Exception exception =
        ErrorReport.wrap(new IllegalArgumentException("Field [x] not found.")).build();
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    Assert.assertFalse(msg instanceof OpenSearchErrorMessage);
    Assert.assertTrue(msg.toString().contains("IllegalArgumentException"));
    Assert.assertFalse(msg.toString().contains("ErrorReport"));
  }

  @Test
  public void errorReportWrappingEsExceptionShouldCreateEsErrorMessage() {
    Exception exception = ErrorReport.wrap(new OpenSearchException(nonOpenSearchThrowable)).build();
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.NOT_FOUND.getStatus());
    Assert.assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void
      nonOpenSearchExceptionWithMultiLayerWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = new Exception(new Throwable(new Throwable(openSearchThrowable)));
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    Assert.assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  private static final OpenSearchSecurityException DENIED =
      new OpenSearchSecurityException(
          "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN);

  private static final QueryShardException BAD_VALUE =
      new QueryShardException(
          new Index("logs", "_na_"), "failed to create query: For input string: \"abc\"", null);

  @Test
  public void findsClientErrorOnTop() {
    assertSame(DENIED, ErrorMessageFactory.findClientErrorCause(DENIED).orElseThrow());
  }

  @Test
  public void findsClientErrorUnderWrappers() {
    Exception wrapped =
        new IllegalStateException(
            "Error occurred during join query run",
            new RuntimeException(new ExecutionException(DENIED)));

    assertSame(DENIED, ErrorMessageFactory.findClientErrorCause(wrapped).orElseThrow());
    assertTrue(ErrorMessageFactory.isDeniedOrMissing(wrapped));
  }

  @Test
  public void findsIndexNotFoundAndOtherClientStatuses() {
    IndexNotFoundException notFound = new IndexNotFoundException("logs");
    OpenSearchStatusException badRequest =
        new OpenSearchStatusException("bad request", RestStatus.BAD_REQUEST);

    assertSame(
        notFound,
        ErrorMessageFactory.findClientErrorCause(new RuntimeException(notFound)).orElseThrow());
    assertSame(
        badRequest,
        ErrorMessageFactory.findClientErrorCause(new SQLException(badRequest)).orElseThrow());
  }

  @Test
  public void tooManyRequestsAndServerErrorsAreNotClientErrors() {
    assertEquals(
        Optional.empty(),
        ErrorMessageFactory.findClientErrorCause(
            new RuntimeException(
                new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS))));
    assertEquals(
        Optional.empty(),
        ErrorMessageFactory.findClientErrorCause(
            new RuntimeException(
                new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE))));
  }

  @Test
  public void usesTheFirstOpenSearchExceptionInTheChain() {
    // A wrapper whose own status is 500 decides, as it does for the error body.
    OpenSearchException outer = new OpenSearchException("failed", DENIED);

    assertEquals(RestStatus.INTERNAL_SERVER_ERROR, outer.status());
    assertEquals(Optional.empty(), ErrorMessageFactory.findClientErrorCause(outer));
  }

  @Test
  public void noOpenSearchExceptionIsNotClientError() {
    assertEquals(
        Optional.empty(),
        ErrorMessageFactory.findClientErrorCause(new IllegalArgumentException("bad argument")));
  }

  @Test
  public void onlyDeniedOrMissingAreDeniedOrMissing() {
    assertTrue(
        ErrorMessageFactory.isDeniedOrMissing(
            new OpenSearchSecurityException("unauthenticated", RestStatus.UNAUTHORIZED)));
    assertTrue(ErrorMessageFactory.isDeniedOrMissing(DENIED));
    assertTrue(ErrorMessageFactory.isDeniedOrMissing(new IndexNotFoundException("logs")));
    assertFalse(ErrorMessageFactory.isDeniedOrMissing(new RuntimeException(BAD_VALUE)));
    assertFalse(
        ErrorMessageFactory.isDeniedOrMissing(
            new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS)));
    assertFalse(ErrorMessageFactory.isDeniedOrMissing(new IllegalArgumentException("bad")));
  }

  @Test
  public void logFailureLogsDeniedAtDebug() {
    LogEvent event = logFailure(new IllegalStateException(DENIED));

    assertEquals(Level.DEBUG, event.getLevel());
  }

  @Test
  public void logFailureLogsOtherClientErrorAtWarnWithoutStackTrace() {
    LogEvent event = logFailure(new IllegalStateException(BAD_VALUE));

    assertEquals(Level.WARN, event.getLevel());
    assertNull(event.getThrown());
    assertTrue(
        event.getMessage().getFormattedMessage(),
        event.getMessage().getFormattedMessage().contains("For input string"));
  }

  @Test
  public void logFailureLogsServerErrorAtGivenLevelWithStackTrace() {
    Exception failure =
        new IllegalStateException(
            new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE));

    LogEvent event = logFailure(failure);

    assertEquals(Level.ERROR, event.getLevel());
    assertSame(failure, event.getThrown());
  }

  private static LogEvent logFailure(Exception failure) {
    List<LogEvent> events = new ArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("capture", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    Logger logger = (Logger) LogManager.getLogger(ErrorMessageFactoryTest.class);
    Level originalLevel = logger.getLevel();
    Configurator.setLevel(logger.getName(), Level.DEBUG);
    logger.addAppender(appender);
    try {
      ErrorMessageFactory.logFailure(logger, Level.ERROR, "Failed during join query run.", failure);
    } finally {
      logger.removeAppender(appender);
      appender.stop();
      Configurator.setLevel(logger.getName(), originalLevel);
    }
    assertEquals(events.toString(), 1, events.size());
    return events.get(0);
  }
}
