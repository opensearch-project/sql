/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.QueryEngineException;

/**
 * Verifies {@link RestSqlAction#getRestStatus(Exception)} classifies unsupported-feature errors as
 * a client error (4xx) instead of letting them leak as HTTP 500, while leaving the existing
 * index-not-found (404) and server-fault (500) behavior unchanged.
 */
public class RestSqlActionErrorStatusTest {

  /**
   * The fix: a {@link QueryEngineException} (e.g. {@link CalciteUnsupportedException} for an
   * unsupported SQL table function such as {@code vectorSearch()}) is a client error and must map
   * to 400, matching the PPL path. Previously it leaked as 500.
   */
  @Test
  public void queryEngineExceptionIsClientError400() {
    QueryEngineException ex = new CalciteUnsupportedException("Table function is unsupported");
    assertEquals(RestStatus.BAD_REQUEST, RestSqlAction.getRestStatus(ex));
  }

  /**
   * An {@link ErrorReport} wrapping a {@link CalciteUnsupportedException} (the shape produced by
   * {@code visitTableFunction}) unwraps to its client-error cause and maps to 400.
   */
  @Test
  public void errorReportWrappingCalciteUnsupportedIsClientError400() {
    ErrorReport report =
        ErrorReport.wrap(new CalciteUnsupportedException("Table function is unsupported"))
            .code(ErrorCode.UNSUPPORTED_OPERATION)
            .build();
    assertEquals(RestStatus.BAD_REQUEST, RestSqlAction.getRestStatus(report));
  }

  /**
   * Unchanged behavior: index-not-found stays 404. An {@link IndexNotFoundException} is an
   * OpenSearchException whose status is NOT_FOUND; this PR must not flip that to 400.
   */
  @Test
  public void indexNotFoundStays404() {
    assertEquals(
        RestStatus.NOT_FOUND,
        RestSqlAction.getRestStatus(new IndexNotFoundException("nonexistent")));
  }

  /**
   * Unchanged behavior: an ErrorReport wrapping an IndexNotFoundException still unwraps to its
   * cause's NOT_FOUND status (404), not 400.
   */
  @Test
  public void errorReportWrappingIndexNotFoundStays404() {
    ErrorReport report = ErrorReport.wrap(new IndexNotFoundException("nonexistent")).build();
    assertEquals(RestStatus.NOT_FOUND, RestSqlAction.getRestStatus(report));
  }

  /** Unchanged behavior: a genuine server fault still maps to 500. */
  @Test
  public void unrecognizedExceptionIsServerError500() {
    assertEquals(
        RestStatus.INTERNAL_SERVER_ERROR,
        RestSqlAction.getRestStatus(new RuntimeException("genuine backend fault")));
  }
}
