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
 * Verifies {@link RestSqlAction#getRestStatus(Exception)} classifies unsupported-feature and other
 * client errors as 4xx instead of letting them leak as HTTP 500. Covers both the exception-type
 * path ({@link #isClientError}) and the structured {@link ErrorReport} / {@link ErrorCode} path.
 */
public class RestSqlActionErrorStatusTest {

  /**
   * Regression for the analytics-engine cursor / vectorSearch() 500s: a {@link
   * QueryEngineException} (e.g. {@link CalciteUnsupportedException} for an unsupported SQL table
   * function) is a client error and must map to 400, matching the PPL path.
   */
  @Test
  public void queryEngineExceptionIsClientError400() {
    QueryEngineException ex = new CalciteUnsupportedException("Table function is unsupported");
    assertEquals(RestStatus.BAD_REQUEST, RestSqlAction.getRestStatus(ex));
  }

  /** An ErrorReport carrying UNSUPPORTED_OPERATION should be a 400 regardless of cause type. */
  @Test
  public void errorReportUnsupportedOperationIsClientError400() {
    ErrorReport report =
        ErrorReport.wrap(new RuntimeException("unsupported"))
            .code(ErrorCode.UNSUPPORTED_OPERATION)
            .build();
    assertEquals(RestStatus.BAD_REQUEST, RestSqlAction.getRestStatus(report));
  }

  /**
   * An ErrorReport wrapping a CalciteUnsupportedException (the shape produced by
   * visitTableFunction) maps to 400 — exercises both the ErrorCode path and the cause-unwrap
   * fallback.
   */
  @Test
  public void errorReportWrappingCalciteUnsupportedIsClientError400() {
    ErrorReport report =
        ErrorReport.wrap(new CalciteUnsupportedException("Table function is unsupported"))
            .code(ErrorCode.UNSUPPORTED_OPERATION)
            .build();
    assertEquals(RestStatus.BAD_REQUEST, RestSqlAction.getRestStatus(report));
  }

  /** ErrorReport.PERMISSION_DENIED maps to 403. */
  @Test
  public void errorReportPermissionDeniedIs403() {
    ErrorReport report =
        ErrorReport.wrap(new RuntimeException("denied")).code(ErrorCode.PERMISSION_DENIED).build();
    assertEquals(RestStatus.FORBIDDEN, RestSqlAction.getRestStatus(report));
  }

  /**
   * When an ErrorReport's code carries no status opinion (e.g. EXECUTION_ERROR), classification
   * falls back to the wrapped cause. A non-client cause stays 500.
   */
  @Test
  public void errorReportWithoutClientCodeFallsBackToCause500() {
    ErrorReport report =
        ErrorReport.wrap(new RuntimeException("boom")).code(ErrorCode.EXECUTION_ERROR).build();
    assertEquals(RestStatus.INTERNAL_SERVER_ERROR, RestSqlAction.getRestStatus(report));
  }

  /**
   * Fallback path: an ErrorReport whose code carries no opinion but whose cause is a recognized
   * client error (IndexNotFoundException is an OpenSearchException with NOT_FOUND status) keeps the
   * cause's status.
   */
  @Test
  public void errorReportFallbackHonorsCauseStatus404() {
    ErrorReport report =
        ErrorReport.wrap(new IndexNotFoundException("nonexistent")).code(ErrorCode.UNKNOWN).build();
    assertEquals(RestStatus.NOT_FOUND, RestSqlAction.getRestStatus(report));
  }

  /** A genuine server fault (unrecognized runtime exception) still maps to 500. */
  @Test
  public void unrecognizedExceptionIsServerError500() {
    assertEquals(
        RestStatus.INTERNAL_SERVER_ERROR,
        RestSqlAction.getRestStatus(new RuntimeException("genuine backend fault")));
  }
}
