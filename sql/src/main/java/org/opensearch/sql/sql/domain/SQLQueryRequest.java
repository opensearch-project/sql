/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.domain;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.json.JSONObject;
import org.opensearch.sql.protocol.response.format.Format;

/** SQL query request. */
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class SQLQueryRequest {
  private static final String QUERY_FIELD_CURSOR = "cursor";
  private static final Set<String> SUPPORTED_FIELDS =
      Set.of("query", "fetch_size", "parameters", QUERY_FIELD_CURSOR);
  private static final String QUERY_PARAMS_FORMAT = "format";
  private static final String QUERY_PARAMS_SANITIZE = "sanitize";
  private static final String QUERY_PARAMS_PRETTY = "pretty";

  /** JSON payload in REST request. */
  private final JSONObject jsonContent;

  /** SQL query. */
  @Getter private final String query;

  /** Request path. */
  private final String path;

  /** Request format. */
  private final String format;

  /** Request params. */
  private Map<String, String> params = Collections.emptyMap();

  @Getter
  @Accessors(fluent = true)
  private boolean sanitize = true;

  private String cursor;

  /** Constructor of SQLQueryRequest that passes request params. */
  public SQLQueryRequest(
      JSONObject jsonContent,
      String query,
      String path,
      Map<String, String> params,
      String cursor) {
    this.jsonContent = jsonContent;
    this.query = query;
    this.path = path;
    this.params = params;
    this.format = getFormat(params);
    this.sanitize = shouldSanitize(params);
    this.cursor = cursor;
  }

  /**
   *
   *
   * <pre>
   * Pre-check if the request can be supported by meeting ALL the following criteria:
   *  1.Only supported fields present in request body, ex. "filter" and "cursor" are not supported
   *  2.Response format is default or can be supported.
   * </pre>
   *
   * @return true if supported.
   */
  public boolean isSupported() {
    boolean hasCursor = isCursor();
    boolean hasQuery = query != null;
    boolean hasContent = jsonContent != null && !jsonContent.isEmpty();

    Predicate<String> supportedParams =
        List.of(QUERY_PARAMS_FORMAT, QUERY_PARAMS_PRETTY)::contains;
    boolean hasUnsupportedParams =
        (!params.isEmpty())
            && params.keySet().stream().dropWhile(supportedParams).findAny().isPresent();

    boolean validCursor = hasCursor && !hasQuery && !hasUnsupportedParams && !hasContent;
    boolean validQuery = !hasCursor && hasQuery;

    return (validCursor || validQuery) // It's a valid cursor or a valid query
        && isOnlySupportedFieldInPayload() // and request must contain supported fields only
        && isSupportedFormat(); // and request must be a supported format
  }

  private boolean isCursor() {
    return cursor != null && !cursor.isEmpty();
  }

  /**
   * Check if request is to explain rather than execute the query.
   *
   * @return true if it is an explain request
   */
  public boolean isExplainRequest() {
    return path.endsWith("/_explain");
  }

  public boolean isCursorCloseRequest() {
    return path.endsWith("/close");
  }

  /** Decide on the formatter by the requested format. */
  public Format format() {
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "response in %s format is not supported.", format));
    }
  }

  private boolean isOnlySupportedFieldInPayload() {
    return jsonContent == null || SUPPORTED_FIELDS.containsAll(jsonContent.keySet());
  }

  public Optional<String> getCursor() {
    return Optional.ofNullable(cursor);
  }

  public int getFetchSize() {
    return jsonContent.optInt("fetch_size");
  }

  private boolean isSupportedFormat() {
    return Stream.of("csv", "jdbc", "raw").anyMatch(format::equalsIgnoreCase);
  }

  private String getFormat(Map<String, String> params) {
    return params.getOrDefault(QUERY_PARAMS_FORMAT, "jdbc");
  }

  private boolean shouldSanitize(Map<String, String> params) {
    if (params.containsKey(QUERY_PARAMS_SANITIZE)) {
      return Boolean.parseBoolean(params.get(QUERY_PARAMS_SANITIZE));
    }
    return true;
  }
}
