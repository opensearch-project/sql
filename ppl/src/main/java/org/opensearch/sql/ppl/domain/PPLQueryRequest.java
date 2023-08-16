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
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

public class PPLQueryRequest {

  private static final String DEFAULT_PPL_PATH = "/_plugins/_ppl";

  public static final PPLQueryRequest NULL = new PPLQueryRequest("", null, DEFAULT_PPL_PATH, "");

  private final String pplQuery;
  @Getter private final JSONObject jsonContent;
  @Getter private final String path;
  @Getter private String format = "";

  @Setter
  @Getter
  @Accessors(fluent = true)
  private boolean sanitize = true;

  @Setter
  @Getter
  @Accessors(fluent = true)
  private JsonResponseFormatter.Style style = JsonResponseFormatter.Style.COMPACT;

  public PPLQueryRequest(String pplQuery, JSONObject jsonContent, String path) {
    this(pplQuery, jsonContent, path, "");
  }

  /** Constructor of PPLQueryRequest. */
  public PPLQueryRequest(String pplQuery, JSONObject jsonContent, String path, String format) {
    this.pplQuery = pplQuery;
    this.jsonContent = jsonContent;
    this.path = Optional.ofNullable(path).orElse(DEFAULT_PPL_PATH);
    this.format = format;
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
}
