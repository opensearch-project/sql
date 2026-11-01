/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.StageErrorHandler;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;

/**
 * Shared sync-shape error rendering for the PPL path.
 *
 * <p>Two consumers:
 *
 * <ul>
 *   <li>the sync REST handler ({@link RestPPLQueryAction}) — resolves the HTTP status for a
 *       transport-layer failure;
 *   <li>the async job service — captures the same structured {@code error} object at failure time
 *       so a later {@code GET /_plugins/_async_query/{id}} on a {@code FAILED} job returns the same
 *       error payload the sync path would have returned.
 * </ul>
 *
 * <p>The status mapping is checked in this order:
 *
 * <ol>
 *   <li>an {@link ErrorReport} unwraps to its cause;
 *   <li>an {@link OpenSearchRejectedExecutionException} (a saturated thread pool) maps to 429;
 *   <li>an {@link OpenSearchException}, on top or with only {@code SEARCH_FAILURE_WRAPPERS} above
 *       it, contributes its own status. A {@link SearchPhaseExecutionException} whose chain {@link
 *       StageErrorHandler#isCodegenFailure(Throwable)} matches maps to 500 instead;
 *   <li>otherwise the top exception's own type decides, and a known client-error type maps to 400;
 *   <li>everything else maps to 500.
 * </ol>
 */
public final class SyncErrorReportRenderer {

  // The wrappers a failed search arrives in. The first fetch, and any fetch under a blocking
  // operator, runs inside executeQuery and arrives in all four. A later fetch runs inside
  // ResultSet.next() and arrives without RuntimeException and SQLException. Matched by exact
  // class, so a subclass such as the IllegalStateException from getIndexMappings stops the walk.
  private static final Set<Class<?>> SEARCH_FAILURE_WRAPPERS =
      Set.of(
          RuntimeException.class, // OpenSearchExecutionEngine.execute, and any other plain wrap
          SQLException.class, // Avatica, around a failure inside executeQuery
          NonFallbackCalciteException.class, // BackgroundSearchScanner, also a client-error type
          ExecutionException.class); // the background fetch's CompletableFuture

  private SyncErrorReportRenderer() {}

  /**
   * Resolves the HTTP status an exception would surface through the sync PPL path. See the class
   * javadoc for the exact mapping.
   */
  public static int statusCodeFor(Throwable throwable) {
    if (throwable instanceof ErrorReport report) {
      return statusCodeFor(report.getCause());
    }
    Throwable firstNonWrapper =
        ExceptionUtils.getThrowableList(throwable).stream()
            .filter(t -> !SEARCH_FAILURE_WRAPPERS.contains(t.getClass()))
            .findFirst()
            .orElse(null);
    if (firstNonWrapper instanceof OpenSearchRejectedExecutionException) {
      return 429;
    }
    if (firstNonWrapper instanceof OpenSearchException openSearchCause) {
      // A failed search runs only while the query executes, so this check stands in for the stage
      // check isCodegenFailure leaves to its caller.
      return openSearchCause instanceof SearchPhaseExecutionException
              && StageErrorHandler.isCodegenFailure(throwable)
          ? 500
          : openSearchCause.status().getStatus();
    }
    if (isClientError(throwable)) {
      return 400;
    }
    return 500;
  }

  /**
   * Produces the structured error map that normally sits under {@code "error"} in the sync HTTP
   * body. The outer {@code "status"} HTTP envelope is deliberately stripped so the returned map can
   * be embedded alongside an async {@code status} field without colliding.
   */
  public static Map<String, Object> renderErrorMap(Throwable throwable) {
    int status = statusCodeFor(throwable);
    return ErrorMessageFactory.createErrorMessage(throwable, status).getErrorAsJson().toMap();
  }

  private static boolean isClientError(Throwable throwable) {
    // (Tombstone) NullPointerException has historically been treated as a client error, but
    // nowadays they're rare and should be treated as system errors, since it represents a broken
    // data model in our logic.
    return throwable instanceof IllegalArgumentException
        || throwable instanceof IndexNotFoundException
        || throwable instanceof QueryEngineException
        || throwable instanceof SyntaxCheckException
        || throwable instanceof DataSourceClientException
        || throwable instanceof IllegalAccessException;
  }
}
