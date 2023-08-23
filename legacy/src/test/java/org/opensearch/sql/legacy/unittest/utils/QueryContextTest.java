/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.utils;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;

import org.apache.logging.log4j.ThreadContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.common.utils.QueryContext;

public class QueryContextTest {

  private static final String REQUEST_ID_KEY = "request_id";

  @After
  public void cleanUpContext() {

    ThreadContext.clearMap();
  }

  @Test
  public void addRequestId() {

    Assert.assertNull(ThreadContext.get(REQUEST_ID_KEY));
    QueryContext.addRequestId();
    final String requestId = ThreadContext.get(REQUEST_ID_KEY);
    Assert.assertNotNull(requestId);
  }

  @Test
  public void addRequestId_alreadyExists() {

    QueryContext.addRequestId();
    final String requestId = ThreadContext.get(REQUEST_ID_KEY);
    QueryContext.addRequestId();
    final String requestId2 = ThreadContext.get(REQUEST_ID_KEY);
    Assert.assertThat(requestId2, not(equalTo(requestId)));
  }

  @Test
  public void getRequestId_doesNotExist() {
    assertNotNull(QueryContext.getRequestId());
  }

  @Test
  public void getRequestId() {

    final String test_request_id = "test_id_111";
    ThreadContext.put(REQUEST_ID_KEY, test_request_id);
    final String requestId = QueryContext.getRequestId();
    Assert.assertThat(requestId, equalTo(test_request_id));
  }

  @Test
  public void withCurrentContext() throws InterruptedException {

    Runnable task =
        () -> {
          Assert.assertTrue(ThreadContext.containsKey("test11"));
          Assert.assertTrue(ThreadContext.containsKey("test22"));
        };
    ThreadContext.put("test11", "value11");
    ThreadContext.put("test22", "value11");
    new Thread(QueryContext.withCurrentContext(task)).join();
  }
}
