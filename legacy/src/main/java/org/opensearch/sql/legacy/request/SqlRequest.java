/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.request;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.util.Collections;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.sql.legacy.exception.SqlParseException;

public class SqlRequest {

  public static final SqlRequest NULL = new SqlRequest("", null);

  String sql;
  JSONObject jsonContent;
  String cursor;
  Integer fetchSize;

  public SqlRequest(final String sql, final JSONObject jsonContent) {
    this.sql = sql;
    this.jsonContent = jsonContent;
  }

  public SqlRequest(final String cursor) {
    this.cursor = cursor;
  }

  public SqlRequest(final String sql, final Integer fetchSize, final JSONObject jsonContent) {
    this.sql = sql;
    this.fetchSize = fetchSize;
    this.jsonContent = jsonContent;
  }

  private static boolean isValidJson(String json) {
    try {
      new JSONObject(json);
    } catch (JSONException e) {
      return false;
    }
    return true;
  }

  public String getSql() {
    return this.sql;
  }

  public String cursor() {
    return this.cursor;
  }

  public Integer fetchSize() {
    return this.fetchSize;
  }

  public JSONObject getJsonContent() {
    return this.jsonContent;
  }

  /**
   * JSONObject's getJSONObject method will return just the value, this helper method is to extract
   * the key and value of 'filter' and return the JSON as a string.
   */
  private String getFilterObjectAsString(JSONObject jsonContent) {
    String filterVal = jsonContent.getJSONObject("filter").toString();
    return "{\"filter\":" + filterVal + "}";
  }

  private boolean hasFilterInRequest() {
    return jsonContent != null && jsonContent.has("filter");
  }

  /**
   * Takes 'filter' parameter from JSON request if JSON request and 'filter' were given and creates
   * a QueryBuilder object out of it to add to the filterClauses of the BoolQueryBuilder.
   */
  private void addFilterFromJson(BoolQueryBuilder boolQuery) throws SqlParseException {
    try {
      String filter = getFilterObjectAsString(jsonContent);
      SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
      XContentParser parser =
          new JsonXContentParser(
              new NamedXContentRegistry(searchModule.getNamedXContents()),
              LoggingDeprecationHandler.INSTANCE,
              new JsonFactory().createParser(filter));

      // nextToken is called before passing the parser to fromXContent since the fieldName will be
      // null if the
      // first token it parses is START_OBJECT resulting in an exception
      parser.nextToken();
      boolQuery.filter(BoolQueryBuilder.fromXContent(parser));
    } catch (IOException e) {
      throw new SqlParseException("Unable to parse 'filter' in JSON request: " + e.getMessage());
    }
  }

  public BoolQueryBuilder checkAndAddFilter(BoolQueryBuilder boolQuery) throws SqlParseException {
    if (hasFilterInRequest()) {
      // if WHERE was not given, create a new BoolQuery to add "filter" to
      boolQuery = boolQuery == null ? new BoolQueryBuilder() : boolQuery;
      addFilterFromJson(boolQuery);
    }
    return boolQuery;
  }
}
