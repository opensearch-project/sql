/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;
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
}
