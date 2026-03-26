/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;

public class ErrorMessageFactoryTest {
  private final Throwable nonOpenSearchThrowable = new Throwable();
  private final Throwable openSearchThrowable = new OpenSearchException(nonOpenSearchThrowable);

  @Test
  public void openSearchExceptionShouldCreateEsErrorMessage() {
    Exception exception = new OpenSearchException(nonOpenSearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void nonOpenSearchExceptionShouldCreateGenericErrorMessage() {
    Exception exception = new Exception(nonOpenSearchThrowable);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertFalse(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void nonOpenSearchExceptionWithWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = (Exception) openSearchThrowable;
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void
      nonOpenSearchExceptionWithMultiLayerWrappedEsExceptionCauseShouldCreateEsErrorMessage() {
    Exception exception = new Exception(new Throwable(new Throwable(openSearchThrowable)));
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(exception, RestStatus.BAD_REQUEST.getStatus());
    assertTrue(msg instanceof OpenSearchErrorMessage);
  }

  @Test
  public void wrappedTaskCancelledExceptionShouldCreateGenericErrorMessageWithPassedStatus() {
    TaskCancelledException cancelled = new TaskCancelledException("The task is cancelled.");
    SQLException sqlEx = new SQLException("exception while executing query", cancelled);
    RuntimeException wrapped = new RuntimeException(sqlEx);
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(wrapped, RestStatus.BAD_REQUEST.getStatus());
    assertFalse(msg instanceof OpenSearchErrorMessage);
    assertTrue(msg.toString().contains("\"status\": 400"));
  }

  @Test
  public void directTaskCancelledExceptionShouldCreateGenericErrorMessageWithPassedStatus() {
    TaskCancelledException cancelled = new TaskCancelledException("The task is cancelled.");
    ErrorMessage msg =
        ErrorMessageFactory.createErrorMessage(cancelled, RestStatus.BAD_REQUEST.getStatus());
    assertFalse(msg instanceof OpenSearchErrorMessage);
    assertTrue(msg.toString().contains("\"status\": 400"));
  }
}
