/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import java.util.Map;
import org.opensearch.OpenSearchException;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
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
 * <p>The status-mapping logic is intentionally identical to what {@link RestPPLQueryAction} used to
 * carry privately: {@link ErrorReport} unwraps to its cause, {@link OpenSearchException}
 * contributes its own status, a known client-error type maps to 400, everything else maps to 500.
 * Preserving this mapping is required for sync REST behavior to stay byte-identical after the
 * refactor.
 */
public final class SyncErrorReportRenderer {

  private SyncErrorReportRenderer() {}

  /**
   * Resolves the HTTP status an exception would surface through the sync PPL path. See the class
   * javadoc for the exact mapping.
   */
  public static int statusCodeFor(Throwable throwable) {
    if (throwable instanceof ErrorReport report) {
      return statusCodeFor(report.getCause());
    }
    if (throwable instanceof OpenSearchException openSearchException) {
      return openSearchException.status().getStatus();
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
