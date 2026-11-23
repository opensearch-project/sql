/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.utils;

import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;

/**
 * Rules for failures that OpenSearch reports to the legacy engine. They decide which failure is a
 * client error, how a failure is logged and which failure metric it counts.
 */
@UtilityClass
public class OpenSearchFailures {

  /**
   * Walks the cause chain to the first {@link OpenSearchException}. The error body is built from
   * this exception.
   *
   * @param failure the failure, possibly wrapped
   * @return the first OpenSearch exception in the cause chain, or the root cause if there is none
   */
  public static Throwable unwrapToOpenSearchCause(Throwable failure) {
    if (failure instanceof OpenSearchException || failure.getCause() == null) {
      return failure;
    }
    return unwrapToOpenSearchCause(failure.getCause());
  }

  /**
   * Finds the failure that OpenSearch reported, even when the plugin wrapped it in another
   * exception. It is the first {@link OpenSearchException} in the cause chain, the same one the
   * error body is built from, and it has a status. The response uses this status so that the HTTP
   * status matches the status in the body.
   *
   * @param failure the failure, possibly wrapped
   * @return the OpenSearch exception whose status the response uses, or empty if there is none
   */
  public static Optional<OpenSearchException> findOpenSearchCause(Throwable failure) {
    Throwable cause = unwrapToOpenSearchCause(failure);
    if (cause instanceof OpenSearchException && ((OpenSearchException) cause).status() != null) {
      return Optional.of((OpenSearchException) cause);
    }
    return Optional.empty();
  }

  /**
   * Finds the failure that OpenSearch reported with a client error status. It is the exception
   * found by {@link #findOpenSearchCause} when its status is 4xx, 429 included.
   *
   * @param failure the failure, possibly wrapped
   * @return the OpenSearch exception with a client error status, or empty if there is none
   */
  public static Optional<OpenSearchException> findClientErrorCause(Throwable failure) {
    return findOpenSearchCause(failure).filter(cause -> isClientError(cause.status().getStatus()));
  }

  /**
   * Tells whether OpenSearch denied the request (401 or 403) or reported something missing (404),
   * such as an index or a point in time. Such a failure fails the same way on every retry. A
   * transport wrapper around one of them counts too, because it reports the status of its cause.
   *
   * @param failure the failure, possibly wrapped
   * @return true if the OpenSearch exception found by {@link #findOpenSearchCause} has status 401,
   *     403 or 404
   */
  public static boolean isDeniedOrMissing(Throwable failure) {
    return findOpenSearchCause(failure)
        .map(OpenSearchException::status)
        .filter(
            status ->
                status == RestStatus.UNAUTHORIZED
                    || status == RestStatus.FORBIDDEN
                    || status == RestStatus.NOT_FOUND)
        .isPresent();
  }

  /**
   * Logs a failed request. An OpenSearch client error, see {@link #findClientErrorCause}, is logged
   * at WARN, or at {@code level} if that is lower, with its detailed message and without the stack
   * trace. Any other failure is logged at {@code level} with the stack trace.
   *
   * @param log the logger to write to
   * @param level the level for a failure that is not an OpenSearch client error
   * @param message the log message
   * @param failure the failure, possibly wrapped
   */
  public static void logFailure(Logger log, Level level, String message, Throwable failure) {
    Optional<OpenSearchException> clientError = findClientErrorCause(failure);
    if (clientError.isPresent()) {
      log.log(
          level.isMoreSpecificThan(Level.WARN) ? Level.WARN : level,
          "{} {}",
          message,
          clientError.get().getDetailedMessage());
    } else {
      log.log(level, message, failure);
    }
  }

  /**
   * Counts a failed request by the status of its response. A 4xx counts as a client failure and a
   * 5xx counts as a system failure. Any other status is not counted.
   *
   * @param status the HTTP status of the response
   */
  public static void countFailure(int status) {
    if (isClientError(status)) {
      Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_CUS).increment();
    } else if (status >= 500 && status < 600) {
      Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
    }
  }

  private static boolean isClientError(int status) {
    return status >= 400 && status < 500;
  }
}
