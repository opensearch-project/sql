/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.ast.tree.HighlightConfig;
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

  @Test
  public void testGetHighlightConfigReturnsNullWhenJsonContentIsNull() {
    PPLQueryRequest request = new PPLQueryRequest("source=t", null, "/_plugins/_ppl");
    assertNull(request.getHighlightConfig());
  }

  @Test
  public void testGetHighlightConfigReturnsNullWhenNotSpecified() {
    JSONObject json = new JSONObject("{\"query\": \"source=t\"}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    assertNull(request.getHighlightConfig());
  }

  @Test
  public void testGetHighlightConfigSimpleArrayWildcard() {
    JSONObject json = new JSONObject("{\"query\": \"source=t\", \"highlight\": [\"*\"]}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    HighlightConfig config = request.getHighlightConfig();
    assertNotNull(config);
    assertEquals(List.of("*"), config.fieldNames());
    assertNull(config.preTags());
    assertNull(config.postTags());
    assertNull(config.fragmentSize());
  }

  @Test
  public void testGetHighlightConfigSimpleArrayMultipleTerms() {
    JSONObject json =
        new JSONObject("{\"query\": \"source=t\", \"highlight\": [\"error\", \"login\"]}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    HighlightConfig config = request.getHighlightConfig();
    assertNotNull(config);
    assertEquals(List.of("error", "login"), config.fieldNames());
    assertNull(config.preTags());
    assertNull(config.postTags());
    assertNull(config.fragmentSize());
  }

  @Test
  public void testGetHighlightConfigOsdObjectFormatAllFields() {
    JSONObject json =
        new JSONObject(
            "{\"query\": \"source=t\", \"highlight\": {"
                + "\"pre_tags\": [\"<b>\"], \"post_tags\": [\"</b>\"],"
                + "\"fields\": {\"*\": {}}, \"fragment_size\": 2147483647}}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    HighlightConfig config = request.getHighlightConfig();
    assertNotNull(config);
    assertEquals(List.of("*"), config.fieldNames());
    assertEquals(List.of("<b>"), config.preTags());
    assertEquals(List.of("</b>"), config.postTags());
    assertEquals(Integer.valueOf(2147483647), config.fragmentSize());
  }

  @Test
  public void testGetHighlightConfigOsdObjectFormatMultipleTags() {
    JSONObject json =
        new JSONObject(
            "{\"query\": \"source=t\", \"highlight\": {"
                + "\"pre_tags\": [\"<em>\", \"<b>\"], \"post_tags\": [\"</em>\", \"</b>\"],"
                + "\"fields\": {\"title\": {}, \"body\": {}}}}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    HighlightConfig config = request.getHighlightConfig();
    assertNotNull(config);
    assertEquals(2, config.fields().size());
    assertTrue(config.fields().containsKey("title"));
    assertTrue(config.fields().containsKey("body"));
    assertEquals(List.of("<em>", "<b>"), config.preTags());
    assertEquals(List.of("</em>", "</b>"), config.postTags());
    assertNull(config.fragmentSize());
  }

  @Test
  public void testGetHighlightConfigOsdObjectFormatFieldsOnly() {
    // Object format with only fields, no tags or fragment_size
    JSONObject json =
        new JSONObject("{\"query\": \"source=t\", \"highlight\": {\"fields\": {\"*\": {}}}}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    HighlightConfig config = request.getHighlightConfig();
    assertNotNull(config);
    assertEquals(List.of("*"), config.fieldNames());
    assertNull(config.preTags());
    assertNull(config.postTags());
    assertNull(config.fragmentSize());
  }

  @Test
  public void testGetHighlightConfigPerFieldOptions() {
    JSONObject json =
        new JSONObject(
            "{\"query\": \"source=t\", \"highlight\": {"
                + "\"fields\": {\"title\": {\"fragment_size\": 200, \"number_of_fragments\": 3},"
                + " \"body\": {\"type\": \"plain\"}}}}");
    PPLQueryRequest request = new PPLQueryRequest("source=t", json, "/_plugins/_ppl");
    HighlightConfig config = request.getHighlightConfig();
    assertNotNull(config);
    assertEquals(2, config.fields().size());
    assertEquals(200, config.fields().get("title").get("fragment_size"));
    assertEquals(3, config.fields().get("title").get("number_of_fragments"));
    assertEquals("plain", config.fields().get("body").get("type"));
  }
}
