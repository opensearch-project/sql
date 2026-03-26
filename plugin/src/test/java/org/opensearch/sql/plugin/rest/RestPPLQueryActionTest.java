/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.sql.SQLException;
import org.junit.Test;
import org.opensearch.core.tasks.TaskCancelledException;

public class RestPPLQueryActionTest {

  @Test
  public void testTaskCancelledExceptionIsClientError() throws Exception {
    TaskCancelledException cancelled = new TaskCancelledException("The task is cancelled.");
    SQLException sqlEx = new SQLException("exception while executing query", cancelled);
    RuntimeException wrapped = new RuntimeException(sqlEx);

    assertTrue(invokeIsClientError(wrapped));
  }

  @Test
  public void testDirectTaskCancelledExceptionIsClientError() throws Exception {
    TaskCancelledException cancelled = new TaskCancelledException("The task is cancelled.");
    assertTrue(invokeIsClientError(cancelled));
  }

  @Test
  public void testGenericRuntimeExceptionIsNotClientError() throws Exception {
    RuntimeException e = new RuntimeException("something went wrong");
    assertFalse(invokeIsClientError(e));
  }

  private static boolean invokeIsClientError(Exception e) throws Exception {
    Method method = RestPPLQueryAction.class.getDeclaredMethod("isClientError", Exception.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, e);
  }
}
