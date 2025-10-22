/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/*
 * @opensearch.experimental
 */
public class ExecuteDirectQueryResponseTest {

  @Test
  public void testDefaultConstructor() {
    ExecuteDirectQueryResponse response = new ExecuteDirectQueryResponse();
    assertNull(response.getQueryId());
    assertNull(response.getResult());
    assertNull(response.getSessionId());
    assertNull(response.getDataSourceType());
  }

  @Test
  public void testAllArgsConstructor() {
    String queryId = "query-123";
    String result = "query result";
    String sessionId = "session-456";
    String dataSourceType = "prometheus";

    ExecuteDirectQueryResponse response = new ExecuteDirectQueryResponse(
        queryId, result, sessionId, dataSourceType);

    assertEquals(queryId, response.getQueryId());
    assertEquals(result, response.getResult());
    assertEquals(sessionId, response.getSessionId());
    assertEquals(dataSourceType, response.getDataSourceType());
  }

  @Test
  public void testSettersAndGetters() {
    ExecuteDirectQueryResponse response = new ExecuteDirectQueryResponse();

    response.setQueryId("query-789");
    assertEquals("query-789", response.getQueryId());

    response.setResult("test result data");
    assertEquals("test result data", response.getResult());

    response.setSessionId("session-abc");
    assertEquals("session-abc", response.getSessionId());

    response.setDataSourceType("cloudwatch");
    assertEquals("cloudwatch", response.getDataSourceType());
  }

  @Test
  public void testLombokDataAnnotation() {
    ExecuteDirectQueryResponse response1 = new ExecuteDirectQueryResponse(
        "query-1", "result-1", "session-1", "prometheus");
    ExecuteDirectQueryResponse response2 = new ExecuteDirectQueryResponse(
        "query-1", "result-1", "session-1", "prometheus");

    assertEquals(response1, response2);
    assertEquals(response1.hashCode(), response2.hashCode());
    assertEquals(response1.toString(), response2.toString());
  }

  @Test
  public void testWithNullValues() {
    ExecuteDirectQueryResponse response = new ExecuteDirectQueryResponse(
        null, null, null, null);

    assertNull(response.getQueryId());
    assertNull(response.getResult());
    assertNull(response.getSessionId());
    assertNull(response.getDataSourceType());
  }
}