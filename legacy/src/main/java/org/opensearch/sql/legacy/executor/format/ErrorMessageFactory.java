/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import org.opensearch.OpenSearchException;

public class ErrorMessageFactory {
  /**
   * Create error message based on the exception type Exceptions of OpenSearch exception type and
   * exceptions with wrapped OpenSearch exception causes should create {@link
   * OpenSearchErrorMessage}
   *
   * @param e exception to create error message
   * @param status exception status code
   * @return error message
   */
  public static ErrorMessage createErrorMessage(Exception e, int status) {
    if (e instanceof OpenSearchException) {
      return new OpenSearchErrorMessage(
          (OpenSearchException) e, ((OpenSearchException) e).status().getStatus());
    } else if (unwrapCause(e) instanceof OpenSearchException) {
      OpenSearchException exception = (OpenSearchException) unwrapCause(e);
      return new OpenSearchErrorMessage(exception, exception.status().getStatus());
    }
    return new ErrorMessage(e, status);
  }

  public static Throwable unwrapCause(Throwable t) {
    Throwable result = t;
    if (result instanceof OpenSearchException) {
      return result;
    }
    if (result.getCause() == null) {
      return result;
    }
    result = unwrapCause(result.getCause());
    return result;
  }
}
