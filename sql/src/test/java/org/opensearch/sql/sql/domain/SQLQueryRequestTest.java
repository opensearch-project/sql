/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.domain;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.protocol.response.format.Format;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class SQLQueryRequestTest {

  @Test
  public void should_support_query() {
    SQLQueryRequest request = SQLQueryRequestBuilder.request("SELECT 1").build();
    assertTrue(request.isSupported());
  }

  @Test
  public void should_support_query_with_JDBC_format() {
    SQLQueryRequest request = SQLQueryRequestBuilder.request("SELECT 1").format("jdbc").build();
    assertAll(
        () -> assertTrue(request.isSupported()), () -> assertEquals(request.format(), Format.JDBC));
  }

  @Test
  public void should_support_query_with_query_field_only() {
    SQLQueryRequest request =
        SQLQueryRequestBuilder.request("SELECT 1").jsonContent("{\"query\": \"SELECT 1\"}").build();
    assertTrue(request.isSupported());
  }

  @Test
  public void should_support_query_with_parameters() {
    SQLQueryRequest requestWithContent =
        SQLQueryRequestBuilder.request("SELECT 1")
            .jsonContent("{\"query\": \"SELECT 1\", \"parameters\":[]}")
            .build();
    SQLQueryRequest requestWithParams =
        SQLQueryRequestBuilder.request("SELECT 1").params(Map.of("one", "two")).build();
    assertAll(
        () -> assertTrue(requestWithContent.isSupported()),
        () -> assertTrue(requestWithParams.isSupported()));
  }

  @Test
  public void should_support_query_without_parameters() {
    SQLQueryRequest requestWithNoParams =
        SQLQueryRequestBuilder.request("SELECT 1").params(Map.of()).build();
    assertTrue(requestWithNoParams.isSupported());
  }

  @Test
  public void should_support_query_with_zero_fetch_size() {
    SQLQueryRequest request =
        SQLQueryRequestBuilder.request("SELECT 1")
            .jsonContent("{\"query\": \"SELECT 1\", \"fetch_size\": 0}")
            .build();
    assertTrue(request.isSupported());
  }

  @Test
  public void should_support_query_with_parameters_and_zero_fetch_size() {
    SQLQueryRequest request =
        SQLQueryRequestBuilder.request("SELECT 1")
            .jsonContent("{\"query\": \"SELECT 1\", \"fetch_size\": 0, \"parameters\":[]}")
            .build();
    assertTrue(request.isSupported());
  }

  @Test
  public void should_support_explain() {
    SQLQueryRequest explainRequest =
        SQLQueryRequestBuilder.request("SELECT 1").path("_plugins/_sql/_explain").build();

    assertAll(
        () -> assertTrue(explainRequest.isExplainRequest()),
        () -> assertTrue(explainRequest.isSupported()));
  }

  @Test
  public void should_support_explain_format() {
    SQLQueryRequest explainRequest =
        SQLQueryRequestBuilder.request("SELECT 1")
            .path("_plugins/_sql/_explain")
            .params(Map.of("format", "simple"))
            .build();

    assertAll(
        () -> assertTrue(explainRequest.isExplainRequest()),
        () -> assertTrue(explainRequest.isSupported()));
  }

  @Test
  public void should_not_support_explain_with_unsupported_explain_format() {
    SQLQueryRequest explainRequest =
        SQLQueryRequestBuilder.request("SELECT 1")
            .path("_plugins/_sql/_explain")
            .params(Map.of("format", "jdbc"))
            .build();

    assertAll(
        () -> assertTrue(explainRequest.isExplainRequest()),
        () -> assertFalse(explainRequest.isSupported()));
  }

  @Test
  public void should_support_cursor_request() {
    SQLQueryRequest fetchSizeRequest =
        SQLQueryRequestBuilder.request("SELECT 1")
            .jsonContent("{\"query\": \"SELECT 1\", \"fetch_size\": 5}")
            .build();

    SQLQueryRequest cursorRequest =
        SQLQueryRequestBuilder.request(null).cursor("abcdefgh...").build();

    assertAll(
        () -> assertTrue(fetchSizeRequest.isSupported()),
        () -> assertTrue(cursorRequest.isSupported()));
  }

  @Test
  public void should_support_cursor_request_with_supported_parameters() {
    SQLQueryRequest fetchSizeRequest =
        SQLQueryRequestBuilder.request("SELECT 1")
            .jsonContent("{\"query\": \"SELECT 1\", \"fetch_size\": 5}")
            .build();

    SQLQueryRequest cursorRequest =
        SQLQueryRequestBuilder.request(null)
            .cursor("abcdefgh...")
            .params(Map.of("format", "csv", "pretty", "true"))
            .build();

    assertAll(
        () -> assertTrue(fetchSizeRequest.isSupported()),
        () -> assertTrue(cursorRequest.isSupported()));
  }

  @Test
  public void should_not_support_cursor_request_with_unsupported_parameters() {
    SQLQueryRequest fetchSizeRequest =
        SQLQueryRequestBuilder.request("SELECT 1")
            .jsonContent("{\"query\": \"SELECT 1\", \"fetch_size\": 5}")
            .build();

    SQLQueryRequest cursorRequest =
        SQLQueryRequestBuilder.request(null)
            .cursor("abcdefgh...")
            .params(Map.of("one", "two"))
            .build();

    assertAll(
        () -> assertTrue(fetchSizeRequest.isSupported()),
        () -> assertFalse(cursorRequest.isSupported()));
  }

  @Test
  public void should_support_cursor_close_request() {
    SQLQueryRequest closeRequest =
        SQLQueryRequestBuilder.request(null).cursor("pewpew").path("_plugins/_sql/close").build();

    SQLQueryRequest emptyCloseRequest =
        SQLQueryRequestBuilder.request(null).cursor("").path("_plugins/_sql/close").build();

    SQLQueryRequest pagingRequest = SQLQueryRequestBuilder.request(null).cursor("pewpew").build();

    assertAll(
        () -> assertTrue(closeRequest.isSupported()),
        () -> assertTrue(closeRequest.isCursorCloseRequest()),
        () -> assertTrue(pagingRequest.isSupported()),
        () -> assertFalse(pagingRequest.isCursorCloseRequest()),
        () -> assertFalse(emptyCloseRequest.isSupported()),
        () -> assertTrue(emptyCloseRequest.isCursorCloseRequest()));
  }

  @Test
  public void should_not_support_request_with_empty_cursor() {
    SQLQueryRequest requestWithEmptyCursor =
        SQLQueryRequestBuilder.request(null).cursor("").build();
    SQLQueryRequest requestWithNullCursor =
        SQLQueryRequestBuilder.request(null).cursor(null).build();
    assertAll(
        () -> assertFalse(requestWithEmptyCursor.isSupported()),
        () -> assertFalse(requestWithNullCursor.isSupported()));
  }

  @Test
  public void should_not_support_request_with_unknown_field() {
    SQLQueryRequest request =
        SQLQueryRequestBuilder.request("SELECT 1").jsonContent("{\"pewpew\": 42}").build();
    assertFalse(request.isSupported());
  }

  @Test
  public void should_not_support_request_with_cursor_and_something_else() {
    SQLQueryRequest requestWithQuery =
        SQLQueryRequestBuilder.request("SELECT 1").cursor("n:12356").build();
    SQLQueryRequest requestWithParams =
        SQLQueryRequestBuilder.request(null).cursor("n:12356").params(Map.of("one", "two")).build();
    SQLQueryRequest requestWithParamsWithFormat =
        SQLQueryRequestBuilder.request(null)
            .cursor("n:12356")
            .params(Map.of("format", "jdbc"))
            .build();
    SQLQueryRequest requestWithParamsWithFormatAnd =
        SQLQueryRequestBuilder.request(null)
            .cursor("n:12356")
            .params(Map.of("format", "jdbc", "something", "else"))
            .build();
    SQLQueryRequest requestWithFetchSize =
        SQLQueryRequestBuilder.request(null)
            .cursor("n:12356")
            .jsonContent("{\"fetch_size\": 5}")
            .build();
    SQLQueryRequest requestWithNoParams =
        SQLQueryRequestBuilder.request(null).cursor("n:12356").params(Map.of()).build();
    SQLQueryRequest requestWithNoContent =
        SQLQueryRequestBuilder.request(null).cursor("n:12356").jsonContent("{}").build();
    assertAll(
        () -> assertFalse(requestWithQuery.isSupported()),
        () -> assertFalse(requestWithParams.isSupported()),
        () -> assertFalse(requestWithFetchSize.isSupported()),
        () -> assertTrue(requestWithNoParams.isSupported()),
        () -> assertTrue(requestWithParamsWithFormat.isSupported()),
        () -> assertFalse(requestWithParamsWithFormatAnd.isSupported()),
        () -> assertTrue(requestWithNoContent.isSupported()));
  }

  @Test
  public void should_use_JDBC_format_by_default() {
    SQLQueryRequest request =
        SQLQueryRequestBuilder.request("SELECT 1").params(ImmutableMap.of()).build();
    assertEquals(request.format(), Format.JDBC);
  }

  @Test
  public void should_support_CSV_format_and_sanitize() {
    SQLQueryRequest csvRequest = SQLQueryRequestBuilder.request("SELECT 1").format("csv").build();
    assertAll(
        () -> assertTrue(csvRequest.isSupported()),
        () -> assertEquals(csvRequest.format(), Format.CSV),
        () -> assertTrue(csvRequest.sanitize()));
  }

  @Test
  public void should_skip_sanitize_if_set_false() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    Map<String, String> params = builder.put("format", "csv").put("sanitize", "false").build();
    SQLQueryRequest csvRequest = SQLQueryRequestBuilder.request("SELECT 1").params(params).build();
    assertAll(
        () -> assertEquals(csvRequest.format(), Format.CSV),
        () -> assertFalse(csvRequest.sanitize()));
  }

  @Test
  public void should_not_support_other_format() {
    SQLQueryRequest csvRequest = SQLQueryRequestBuilder.request("SELECT 1").format("other").build();

    assertAll(
        () -> assertFalse(csvRequest.isSupported()),
        () ->
            assertEquals(
                "response in other format is not supported.",
                assertThrows(IllegalArgumentException.class, csvRequest::format).getMessage()));
  }

  @Test
  public void should_support_raw_format() {
    SQLQueryRequest csvRequest = SQLQueryRequestBuilder.request("SELECT 1").format("raw").build();
    assertTrue(csvRequest.isSupported());
  }

  /** SQL query request build helper to improve test data setup readability. */
  private static class SQLQueryRequestBuilder {
    private String jsonContent;
    private String query;
    private String path = "_plugins/_sql";
    private String format;
    private String cursor;
    private Map<String, String> params = new HashMap<>();

    static SQLQueryRequestBuilder request(String query) {
      SQLQueryRequestBuilder builder = new SQLQueryRequestBuilder();
      builder.query = query;
      return builder;
    }

    SQLQueryRequestBuilder jsonContent(String jsonContent) {
      this.jsonContent = jsonContent;
      return this;
    }

    SQLQueryRequestBuilder path(String path) {
      this.path = path;
      return this;
    }

    SQLQueryRequestBuilder format(String format) {
      this.format = format;
      return this;
    }

    SQLQueryRequestBuilder params(Map<String, String> params) {
      this.params = params;
      return this;
    }

    SQLQueryRequestBuilder cursor(String cursor) {
      this.cursor = cursor;
      return this;
    }

    SQLQueryRequest build() {
      if (format != null) {
        params.put("format", format);
      }
      return new SQLQueryRequest(
          jsonContent == null ? null : new JSONObject(jsonContent), query, path, params, cursor);
    }
  }
}
