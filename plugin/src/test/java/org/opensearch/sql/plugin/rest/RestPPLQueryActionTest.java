/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.QueryEngineException;

/** Unit tests for error status mapping in {@link RestPPLQueryAction}. */
public class RestPPLQueryActionTest {

  /**
   * Invokes the private static getRawErrorCode via reflection so we can verify
   * the status mapping for various exception types.
   */
  private static int getRawErrorCode(Exception ex) throws Exception {
    Method method =
        RestPPLQueryAction.class.getDeclaredMethod("getRawErrorCode", Exception.class);
    method.setAccessible(true);
    return (int) method.invoke(null, ex);
  }

  @Test
  public void testRejectedExecutionReturns429() throws Exception {
    OpenSearchRejectedExecutionException ex =
        new OpenSearchRejectedExecutionException("admission control backpressure", false);
    assertEquals(429, getRawErrorCode(ex));
  }

  @Test
  public void testOpenSearchStatusExceptionPreservesStatus() throws Exception {
    OpenSearchStatusException ex =
        new OpenSearchStatusException("not found", RestStatus.NOT_FOUND);
    assertEquals(404, getRawErrorCode(ex));
  }

  @Test
  public void testIndexNotFoundReturns404() throws Exception {
    // IndexNotFoundException extends OpenSearchException with status NOT_FOUND
    IndexNotFoundException ex = new IndexNotFoundException("missing_index");
    assertEquals(404, getRawErrorCode(ex));
  }

  @Test
  public void testSyntaxCheckExceptionReturns400() throws Exception {
    SyntaxCheckException ex = new SyntaxCheckException("bad syntax");
    assertEquals(400, getRawErrorCode(ex));
  }

  @Test
  public void testQueryEngineExceptionReturns400() throws Exception {
    QueryEngineException ex = new QueryEngineException("semantic error");
    assertEquals(400, getRawErrorCode(ex));
  }

  @Test
  public void testGenericRuntimeExceptionReturns500() throws Exception {
    RuntimeException ex = new RuntimeException("unexpected failure");
    assertEquals(500, getRawErrorCode(ex));
  }
}
