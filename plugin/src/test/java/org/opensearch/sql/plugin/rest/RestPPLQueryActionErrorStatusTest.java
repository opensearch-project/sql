/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.QueryEngineException;

/**
 * Verifies {@link RestPPLQueryAction#getRawErrorCode(Exception)} classifies unsupported-feature and
 * other client errors as 4xx. Mirrors the SQL-path coverage so both engines stay consistent.
 */
public class RestPPLQueryActionErrorStatusTest {

  @Test
  public void queryEngineExceptionIsClientError400() {
    QueryEngineException ex = new CalciteUnsupportedException("AD command is unsupported");
    assertEquals(400, RestPPLQueryAction.getRawErrorCode(ex));
  }

  @Test
  public void errorReportUnsupportedOperationIsClientError400() {
    ErrorReport report =
        ErrorReport.wrap(new RuntimeException("unsupported"))
            .code(ErrorCode.UNSUPPORTED_OPERATION)
            .build();
    assertEquals(400, RestPPLQueryAction.getRawErrorCode(report));
  }

  @Test
  public void errorReportPermissionDeniedIs403() {
    ErrorReport report =
        ErrorReport.wrap(new RuntimeException("denied")).code(ErrorCode.PERMISSION_DENIED).build();
    assertEquals(403, RestPPLQueryAction.getRawErrorCode(report));
  }

  @Test
  public void errorReportWithoutClientCodeFallsBackToCause() {
    ErrorReport report =
        ErrorReport.wrap(new IndexNotFoundException("nonexistent")).code(ErrorCode.UNKNOWN).build();
    assertEquals(404, RestPPLQueryAction.getRawErrorCode(report));
  }

  @Test
  public void unrecognizedExceptionIsServerError500() {
    assertEquals(500, RestPPLQueryAction.getRawErrorCode(new RuntimeException("genuine fault")));
  }
}
