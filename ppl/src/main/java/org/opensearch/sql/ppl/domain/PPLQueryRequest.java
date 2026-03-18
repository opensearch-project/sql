/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

public class PPLQueryRequest {

  private static final String DEFAULT_PPL_PATH = "/_plugins/_ppl";
  private static final String FETCH_SIZE_FIELD = "fetch_size";
  private static final String HIGHLIGHT_FIELD = "highlight";
  private static final int MAX_HIGHLIGHT_FIELDS = 100;
  private static final int MAX_TAG_ENTRIES = 10;

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

  /**
   * Get highlight config from the request. Supports both the simple array format ({@code ["*"]})
   * and the rich OSD object format with {@code pre_tags}, {@code post_tags}, {@code fields}, and
   * {@code fragment_size}.
   *
   * @return highlight configuration, or null if not specified
   */
  public HighlightConfig getHighlightConfig() {
    if (jsonContent == null || !jsonContent.has(HIGHLIGHT_FIELD)) {
      return null;
    }

    // Simple array format: ["*"] or ["error", "login"]
    JSONArray arr = jsonContent.optJSONArray(HIGHLIGHT_FIELD);
    if (arr != null) {
      List<String> fields = parseAndValidateFieldArray(arr);
      return new HighlightConfig(fields);
    }

    // Rich OSD object format:
    // { "pre_tags": [...], "post_tags": [...], "fields": {"*": {}}, "fragment_size": N }
    JSONObject obj = jsonContent.optJSONObject(HIGHLIGHT_FIELD);
    if (obj == null) {
      return null;
    }

    // Parse and validate fields with per-field options
    Map<String, Map<String, Object>> fields = new LinkedHashMap<>();
    JSONObject fieldsObj = obj.optJSONObject("fields");
    if (fieldsObj != null) {
      if (fieldsObj.keySet().size() > MAX_HIGHLIGHT_FIELDS) {
        throw new IllegalArgumentException(
            "highlight fields count exceeds maximum of " + MAX_HIGHLIGHT_FIELDS);
      }
      for (String key : fieldsObj.keySet()) {
        validateFieldName(key);
        Map<String, Object> perFieldOpts = parsePerFieldOptions(fieldsObj.optJSONObject(key));
        fields.put(key, perFieldOpts);
      }
    }

    // Parse and validate tags
    List<String> preTags = parseAndValidateTagArray(obj.optJSONArray("pre_tags"), "pre_tags");
    List<String> postTags = parseAndValidateTagArray(obj.optJSONArray("post_tags"), "post_tags");

    // Parse and validate fragment_size
    Integer fragmentSize = null;
    if (obj.has("fragment_size")) {
      fragmentSize = obj.getInt("fragment_size");
      if (fragmentSize <= 0) {
        throw new IllegalArgumentException("highlight fragment_size must be a positive integer");
      }
    }

    return new HighlightConfig(fields, preTags, postTags, fragmentSize);
  }

  private static List<String> parseAndValidateFieldArray(JSONArray arr) {
    if (arr.length() > MAX_HIGHLIGHT_FIELDS) {
      throw new IllegalArgumentException(
          "highlight fields count exceeds maximum of " + MAX_HIGHLIGHT_FIELDS);
    }
    List<String> fields = new ArrayList<>();
    for (int i = 0; i < arr.length(); i++) {
      String field = arr.getString(i);
      validateFieldName(field);
      fields.add(field);
    }
    return fields;
  }

  private static void validateFieldName(String fieldName) {
    if (fieldName == null || fieldName.trim().isEmpty()) {
      throw new IllegalArgumentException("highlight field name must be a non-empty string");
    }
  }

  private static List<String> parseAndValidateTagArray(JSONArray arr, String paramName) {
    if (arr == null) {
      return null;
    }
    if (arr.length() > MAX_TAG_ENTRIES) {
      throw new IllegalArgumentException(
          "highlight " + paramName + " count exceeds maximum of " + MAX_TAG_ENTRIES);
    }
    List<String> list = new ArrayList<>();
    for (int i = 0; i < arr.length(); i++) {
      list.add(arr.getString(i));
    }
    return list;
  }

  /**
   * Parse per-field highlight options from a JSON object. Supported options align with the
   * OpenSearch highlight API: {@code fragment_size}, {@code number_of_fragments}, {@code type},
   * {@code pre_tags}, {@code post_tags}, {@code require_field_match}, {@code no_match_size}, {@code
   * order}.
   */
  private static Map<String, Object> parsePerFieldOptions(JSONObject fieldObj) {
    if (fieldObj == null || fieldObj.isEmpty()) {
      return Map.of();
    }
    Map<String, Object> opts = new LinkedHashMap<>();
    if (fieldObj.has("fragment_size")) {
      opts.put("fragment_size", fieldObj.getInt("fragment_size"));
    }
    if (fieldObj.has("number_of_fragments")) {
      opts.put("number_of_fragments", fieldObj.getInt("number_of_fragments"));
    }
    if (fieldObj.has("type")) {
      opts.put("type", fieldObj.getString("type"));
    }
    if (fieldObj.has("pre_tags")) {
      opts.put("pre_tags", jsonArrayToStringList(fieldObj.getJSONArray("pre_tags")));
    }
    if (fieldObj.has("post_tags")) {
      opts.put("post_tags", jsonArrayToStringList(fieldObj.getJSONArray("post_tags")));
    }
    if (fieldObj.has("require_field_match")) {
      opts.put("require_field_match", fieldObj.getBoolean("require_field_match"));
    }
    if (fieldObj.has("no_match_size")) {
      opts.put("no_match_size", fieldObj.getInt("no_match_size"));
    }
    if (fieldObj.has("order")) {
      opts.put("order", fieldObj.getString("order"));
    }
    return opts;
  }

  private static List<String> jsonArrayToStringList(JSONArray arr) {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < arr.length(); i++) {
      list.add(arr.getString(i));
    }
    return list;
  }
}
