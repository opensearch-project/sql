/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Optional;
import org.junit.Test;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.spark.rest.model.LangType;

public class DirectQueryRequestConverterTest {

  @Test
  public void testConvertValidRequest() throws Exception {
    // Create a JSON representation of a valid request
    XContentBuilder builder =
        JsonXContent.contentBuilder()
            .startObject()
            .field("datasource", "prometheus")
            .field("query", "up")
            .field("language", "promql")
            .field("sourceVersion", "v1")
            .field("sessionId", "test-session")
            .field("maxResults", 100)
            .field("timeout", 30)
            .startObject("options")
            .field("queryType", "instant")
            .field("time", "1609459200")
            .endObject()
            .endObject();

    XContentParser parser = createParser(builder);

    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    assertNotNull(request);
    assertEquals("prometheus", request.getDataSources());
    assertEquals("up", request.getQuery());
    assertEquals(LangType.PROMQL, request.getLanguage());
    assertEquals("test-session", request.getSessionId());
    assertEquals(Optional.of(100), Optional.of(request.getMaxResults()));
    assertEquals(Optional.of(30), Optional.of(request.getTimeout()));

    PrometheusOptions options = request.getPrometheusOptions();
    assertNotNull(options);
    assertEquals(PrometheusQueryType.INSTANT, options.getQueryType());
    assertEquals("1609459200", options.getTime());
  }

  @Test
  public void testConvertMultipleDataSources() throws Exception {
    // Create a JSON representation with multiple data sources
    XContentBuilder builder =
        JsonXContent.contentBuilder()
            .startObject()
            .field("datasource", "prometheus1,prometheus2,prometheus3")
            .field("query", "up")
            .field("language", "promql")
            .startObject("options")
            .field("time", "1609459200")
            .endObject()
            .endObject();

    XContentParser parser = createParser(builder);

    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    assertNotNull(request);
    assertEquals("prometheus1,prometheus2,prometheus3", request.getDataSources());
    assertEquals("up", request.getQuery());
  }

  @Test
  public void testConvertNullLanguage() throws Exception {
    // Create a JSON representation with multiple data sources
    XContentBuilder builder =
        JsonXContent.contentBuilder()
            .startObject()
            .field("datasource", "datasource")
            .field("query", "mock-query")
            .field("language", (String) null)
            .endObject();

    XContentParser parser = createParser(builder);

    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    assertNotNull(request);
    assertEquals("datasource", request.getDataSources());
    assertEquals("mock-query", request.getQuery());
  }

  @Test
  public void testConvertPromQLRangeQuery() throws Exception {
    // Create a JSON representation of a range query
    XContentBuilder builder =
        JsonXContent.contentBuilder()
            .startObject()
            .field("datasource", "prometheus")
            .field("query", "rate(http_requests_total[5m])")
            .field("language", "promql")
            .startObject("options")
            .field("queryType", "range")
            .field("start", "1609459200")
            .field("end", "1609545600")
            .field("step", "1h")
            .endObject()
            .endObject();

    XContentParser parser = createParser(builder);

    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    assertNotNull(request);
    assertEquals("prometheus", request.getDataSources());
    assertEquals("rate(http_requests_total[5m])", request.getQuery());
    assertEquals(LangType.PROMQL, request.getLanguage());

    PrometheusOptions options = request.getPrometheusOptions();
    assertNotNull(options);
    assertEquals(PrometheusQueryType.RANGE, options.getQueryType());
    assertEquals("1609459200", options.getStart());
    assertEquals("1609545600", options.getEnd());
    assertEquals("1h", options.getStep());
  }

  @Test
  public void testConvertMalformedRequest() throws Exception {
    // Create a malformed JSON
    XContentBuilder builder =
        JsonXContent.contentBuilder()
            .startObject()
            .field("invalid_field", "some value")
            .endObject();

    XContentParser parser = createParser(builder);

    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    // The parser should create a request object even if fields are missing
    assertNotNull(request);
  }

  @Test
  public void testInvalidLanguageType() throws Exception {
    XContentBuilder builder =
        JsonXContent.contentBuilder()
            .startObject()
            .field("datasource", "prometheus")
            .field("query", "up")
            .field("language", "invalid_language")
            .endObject();

    XContentParser parser = createParser(builder);

    // Should throw exception for invalid language type
    assertThrows(Exception.class, () -> DirectQueryRequestConverter.fromXContentParser(parser));
  }

  @Test
  public void testNullOptions() throws Exception {
    XContentBuilder builder =
        JsonXContent.contentBuilder()
            .startObject()
            .field("datasource", "prometheus")
            .field("query", "up")
            .field("language", "promql")
            .endObject();

    XContentParser parser = createParser(builder);

    ExecuteDirectQueryRequest request = DirectQueryRequestConverter.fromXContentParser(parser);

    assertNotNull(request);
    assertEquals("prometheus", request.getDataSources());
    assertEquals("up", request.getQuery());
    assertEquals(LangType.PROMQL, request.getLanguage());

    PrometheusOptions options = request.getPrometheusOptions();
    assertNotNull(options);
    assertNull(options.getTime());
    assertNull(options.getStart());
    assertNull(options.getEnd());
    assertNull(options.getStep());
  }

  private XContentParser createParser(XContentBuilder builder) throws IOException {
    return XContentType.JSON
        .xContent()
        .createParser(null, null, BytesReference.bytes(builder).streamInput());
  }
}
