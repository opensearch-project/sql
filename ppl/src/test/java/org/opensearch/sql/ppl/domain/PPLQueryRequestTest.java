/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.protocol.response.format.Format;

public class PPLQueryRequestTest {

  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void getRequestShouldPass() {
    PPLQueryRequest request = new PPLQueryRequest("source=t a=1", null, null);
    request.getRequest();
  }

  @Test
  public void testExplainRequest() {
    PPLQueryRequest request = new PPLQueryRequest("source=t a=1", null, "/_plugins/_ppl/_explain");
    assertTrue(request.isExplainRequest());
  }

  @Test
  public void testDefaultFormat() {
    PPLQueryRequest request = new PPLQueryRequest("source=test", null, "/_plugins/_ppl");
    assertEquals(request.format(), Format.JDBC);
  }

  @Test
  public void testJDBCFormat() {
    PPLQueryRequest request = new PPLQueryRequest("source=test", null, "/_plugins/_ppl", "jdbc");
    assertEquals(request.format(), Format.JDBC);
  }

  @Test
  public void testCSVFormat() {
    PPLQueryRequest request = new PPLQueryRequest("source=test", null, "/_plugins/_ppl", "csv");
    assertEquals(request.format(), Format.CSV);
  }

  @Test
  public void testUnsupportedFormat() {
    String format = "notsupport";
    PPLQueryRequest request = new PPLQueryRequest("source=test", null, "/_plugins/_ppl", format);
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("response in " + format + " format is not supported.");
    request.format();
  }

  @Test
  public void testGetFetchSizeReturnsValueFromJson() {
    JSONObject json = new JSONObject("{\"query\": \"source=t\", \"fetch_size\": 100}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    assertEquals(100, request.getFetchSize());
  }

  @Test
  public void testGetFetchSizeReturnsZeroWhenNotSpecified() {
    JSONObject json = new JSONObject("{\"query\": \"source=t\"}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    assertEquals(0, request.getFetchSize());
  }

  @Test
  public void testGetFetchSizeReturnsZeroWhenJsonContentIsNull() {
    PPLQueryRequest request = new PPLQueryRequest("source=t", null, "/_plugins/_ppl");
    assertEquals(0, request.getFetchSize());
  }

  @Test
  public void testGetFetchSizeHandlesExplicitNull() {
    JSONObject json = new JSONObject();
    json.put("query", "source=t");
    json.put("fetch_size", JSONObject.NULL);
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    assertEquals(0, request.getFetchSize());
  }

  @Test
  public void testGetFetchSizeWithLargeValue() {
    JSONObject json = new JSONObject("{\"query\": \"source=t\", \"fetch_size\": 15000}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    assertEquals(15000, request.getFetchSize());
  }
}
