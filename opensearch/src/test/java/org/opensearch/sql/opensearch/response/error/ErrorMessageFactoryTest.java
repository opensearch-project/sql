/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;

public class ErrorMessageFactoryTest {
  private Throwable nonOpenSearchThrowable = new Throwable();
  private Throwable openSearchThrowable = new OpenSearchException(nonOpenSearchThrowable);

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
}
