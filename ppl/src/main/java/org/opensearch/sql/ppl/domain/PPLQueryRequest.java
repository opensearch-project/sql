/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.domain;

import java.util.Locale;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.json.JSONObject;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

public class PPLQueryRequest {

  private static final String DEFAULT_PPL_PATH = "/_plugins/_ppl";
  private static final String FETCH_SIZE_FIELD = "fetch_size";

  public static final PPLQueryRequest NULL = new PPLQueryRequest("", null, DEFAULT_PPL_PATH, "");

  private final String pplQuery;
  @Getter private final JSONObject jsonContent;
  @Getter private final String path;
  @Getter private String format = "";
  @Getter private String explainMode;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean sanitize = true;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private JsonResponseFormatter.Style style = JsonResponseFormatter.Style.COMPACT;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean profile = false;

  public PPLQueryRequest(String pplQuery, JSONObject jsonContent, String path) {
    this(pplQuery, jsonContent, path, "");
  }

  public PPLQueryRequest(String pplQuery, JSONObject jsonContent, String path, String format) {
    this(pplQuery, jsonContent, path, format, ExplainMode.STANDARD.getModeName(), false);
  }

  /** Constructor of PPLQueryRequest. */
  public PPLQueryRequest(
      String pplQuery,
      JSONObject jsonContent,
      String path,
      String format,
      String explainMode,
      boolean profile) {
    this.pplQuery = pplQuery;
    this.jsonContent = jsonContent;
    this.path = Optional.ofNullable(path).orElse(DEFAULT_PPL_PATH);
    this.format = format;
    this.explainMode = explainMode;
    this.profile = profile;
  }

  public String getRequest() {
    return pplQuery;
  }

  /**
   * Check if request is to explain rather than execute the query.
   *
   * @return true if it is a explain request
   */
  public boolean isExplainRequest() {
    return path.endsWith("/_explain");
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

  public ExplainMode mode() {
    return ExplainMode.of(explainMode);
  }

  /**
   * Get the maximum number of results to return. Unlike SQL's fetch_size which enables cursor-based
   * pagination, PPL's fetch_size simply limits the response to N rows without cursor support. The
   * effective upper bound is governed by the {@code plugins.query.size_limit} cluster setting
   * (defaults to {@code index.max_result_window}, which is 10000).
   *
   * @return fetch_size value from request, or 0 if not specified (meaning use system default)
   */
  public int getFetchSize() {
    if (jsonContent == null) {
      return 0;
    }
    return jsonContent.optInt(FETCH_SIZE_FIELD, 0);
  }
}
