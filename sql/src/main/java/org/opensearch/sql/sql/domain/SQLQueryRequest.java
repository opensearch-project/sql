/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.domain;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.json.JSONObject;
import org.opensearch.sql.protocol.response.format.Format;

/**
 * SQL query request.
 */
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class SQLQueryRequest {

  private static final Set<String> SUPPORTED_FIELDS = ImmutableSet.of(
      "query", "fetch_size", "parameters");
  private static final String QUERY_PARAMS_FORMAT = "format";
  private static final String QUERY_PARAMS_SANITIZE = "sanitize";

  /**
   * JSON payload in REST request.
   */
  private final JSONObject jsonContent;

  /**
   * SQL query.
   */
  @Getter
  private final String query;

  /**
   * Request path.
   */
  private final String path;

  /**
   * Request format.
   */
  private final String format;

  /**
   * Request params.
   */
  private Map<String, String> params = Collections.emptyMap();

  /**
   * Request params.
   */
  @Getter
  private final int fetchSize;

  @Getter
  @Accessors(fluent = true)
  private boolean sanitize = true;

  /**
   * Constructor of SQLQueryRequest that passes request params.
   */
  public SQLQueryRequest(
      JSONObject jsonContent, String query, String path, String params) {
    this(jsonContent, query, path, params, 0);
  }

  /**
   * Constructor of SQLQueryRequest that passes request params.
   */
  public SQLQueryRequest(
      JSONObject jsonContent, String query, String path, Map<String, String> params) {
    this(jsonContent, query, path, params, 0);
  }

  /**
   * Constructor of SQLQueryRequest that passes request params.
   */
  public SQLQueryRequest(
      JSONObject jsonContent, String query, String path, Map<String, String> params, int fetchSize) {
    this.jsonContent = jsonContent;
    this.query = query;
    this.path = path;
    this.params = params;
    this.fetchSize = fetchSize;
    this.format = getFormat(params);
    this.sanitize = shouldSanitize(params);
  }

  /**
   * Pre-check if the request can be supported by meeting ALL the following criteria:
   *  1.Only supported fields present in request body, ex. "filter" and "cursor" are not supported
   *  2.No fetch_size or "fetch_size=0". In other word, it's not a cursor request
   *  3.Response format is default or can be supported.
   *
   * @return  true if supported.
   */
  public boolean isSupported() {
    return isOnlySupportedFieldInPayload()
        && isSupportedFormat();
  }

  /**
   * Check if request is to explain rather than execute the query.
   * @return  true if it is a explain request
   */
  public boolean isExplainRequest() {
    return path.endsWith("/_explain");
  }

  /**
   * Decide on the formatter by the requested format.
   */
  public Format format() {
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT,"response in %s format is not supported.", format));
    }
  }

  private boolean isOnlySupportedFieldInPayload() {
    return SUPPORTED_FIELDS.containsAll(jsonContent.keySet());
  }

  private boolean isSupportedFormat() {
    return Strings.isNullOrEmpty(format) || "jdbc".equalsIgnoreCase(format)
        || "csv".equalsIgnoreCase(format) || "raw".equalsIgnoreCase(format);
  }

  private String getFormat(Map<String, String> params) {
    if (params.containsKey(QUERY_PARAMS_FORMAT)) {
      return params.get(QUERY_PARAMS_FORMAT);
    }
    return "jdbc";
  }

  private boolean shouldSanitize(Map<String, String> params) {
    if (params.containsKey(QUERY_PARAMS_SANITIZE)) {
      return Boolean.parseBoolean(params.get(QUERY_PARAMS_SANITIZE));
    }
    return true;
  }

}
