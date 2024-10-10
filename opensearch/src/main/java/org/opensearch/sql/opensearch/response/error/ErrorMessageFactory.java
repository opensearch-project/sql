/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.error;

import lombok.experimental.UtilityClass;
import org.opensearch.OpenSearchException;

@UtilityClass
public class ErrorMessageFactory {
  /**
   * Create error message based on the exception type. Exceptions of OpenSearch exception type and
   * exceptions with wrapped OpenSearch exception causes should create {@link
   * OpenSearchErrorMessage}
   *
   * @param e exception to create error message
   * @param status exception status code
   * @return error message
   */
  public static ErrorMessage createErrorMessage(Throwable e, int status) {
    Throwable cause = unwrapCause(e);
    if (cause instanceof OpenSearchException) {
      OpenSearchException exception = (OpenSearchException) cause;
      return new OpenSearchErrorMessage(exception);
    }
    return new ErrorMessage(e, status);
  }

  protected static Throwable unwrapCause(Throwable t) {
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
