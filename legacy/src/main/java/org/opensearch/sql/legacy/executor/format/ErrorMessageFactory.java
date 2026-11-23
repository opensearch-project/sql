/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import java.util.Optional;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.error.ErrorReport;

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
    // Unwrap ErrorReport so the error type reflects the underlying cause, not the wrapper.
    if (e instanceof ErrorReport) {
      return new ErrorMessage(((ErrorReport) e).getCause(), status);
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

  /**
   * Finds the failure that OpenSearch reported, even when the plugin wrapped it in another
   * exception. It is the first {@link OpenSearchException} in the cause chain, the same one the
   * error body is built from, and it has a status. The response should use this status so that the
   * HTTP status matches the status in the body.
   *
   * @param failure the failure, possibly wrapped
   * @return the OpenSearch exception whose status the response should use, or empty if there is
   *     none
   */
  public static Optional<OpenSearchException> findOpenSearchCause(Throwable failure) {
    Throwable cause = unwrapCause(failure);
    if (cause instanceof OpenSearchException && ((OpenSearchException) cause).status() != null) {
      return Optional.of((OpenSearchException) cause);
    }
    return Optional.empty();
  }

  /**
   * Finds the failure that OpenSearch reported with a client error status. It is the exception
   * found by {@link #findOpenSearchCause} when its status is 4xx other than 429. A 429 asks the
   * caller to retry later, so it is left out here.
   *
   * @param failure the failure, possibly wrapped
   * @return the OpenSearch exception with a client error status, or empty if there is none
   */
  public static Optional<OpenSearchException> findClientErrorCause(Throwable failure) {
    return findOpenSearchCause(failure)
        .filter(
            cause -> {
              int status = cause.status().getStatus();
              return status >= 400
                  && status < 500
                  && cause.status() != RestStatus.TOO_MANY_REQUESTS;
            });
  }

  /**
   * Tells whether OpenSearch denied the request (401 or 403) or reported a missing index. Such a
   * failure does not depend on the data a query reads, so it fails the same way on every retry, and
   * it says nothing about the health of the plugin. Other 404 failures, such as a lost point in
   * time, are not included.
   *
   * @param failure the failure, possibly wrapped
   * @return true if the client error found by {@link #findClientErrorCause} is 401 or 403, or is a
   *     missing index, possibly wrapped by the transport layer
   */
  public static boolean isDeniedOrMissing(Throwable failure) {
    return findClientErrorCause(failure)
        .filter(
            cause ->
                cause.status() == RestStatus.UNAUTHORIZED
                    || cause.status() == RestStatus.FORBIDDEN
                    || ExceptionsHelper.unwrapCause(cause) instanceof IndexNotFoundException)
        .isPresent();
  }

  /**
   * Logs a failed request at a level that matches the status OpenSearch reported for it. A denied
   * request or a missing index, see {@link #isDeniedOrMissing}, is logged at DEBUG. Another
   * OpenSearch client error is logged at WARN, or at {@code level} if that is lower, without the
   * stack trace. Any other failure is logged at {@code level} with the stack trace.
   *
   * @param log the logger to write to
   * @param level the level for a failure that is not an OpenSearch client error
   * @param message the log message
   * @param failure the failure, possibly wrapped
   */
  public static void logFailure(Logger log, Level level, String message, Throwable failure) {
    Optional<OpenSearchException> clientError = findClientErrorCause(failure);
    if (clientError.isEmpty()) {
      log.log(level, message, failure);
    } else if (isDeniedOrMissing(failure)) {
      log.debug(message, failure);
    } else {
      log.log(
          level.isMoreSpecificThan(Level.WARN) ? Level.WARN : level,
          "{} {}",
          message,
          clientError.get().getDetailedMessage());
    }
  }
}
