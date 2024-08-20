/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.cursor;

import static org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;
import static org.opensearch.sql.common.setting.Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.executor.format.Schema;

/**
 * Minimum metdata that will be serialized for generating cursorId for<br>
 * SELECT .... FROM .. ORDER BY .... queries
 */
@Getter
@Setter
@NoArgsConstructor
public class DefaultCursor implements Cursor {

  /**
   * Make sure all keys are unique to prevent overriding and as small as possible to make cursor
   * compact
   */
  private static final String FETCH_SIZE = "f";

  private static final String ROWS_LEFT = "l";
  private static final String INDEX_PATTERN = "i";
  private static final String SCROLL_ID = "s";
  private static final String SCHEMA_COLUMNS = "c";
  private static final String FIELD_ALIAS_MAP = "a";
  private static final String PIT_ID = "p";
  private static final String SEARCH_REQUEST = "r";
  private static final String SORT_FIELDS = "h";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * To get mappings for index to check if type is date needed for
   *
   * @see org.opensearch.sql.legacy.executor.format.DateFieldFormatter
   */
  @NonNull private String indexPattern;

  /**
   * List of Schema.Column for maintaining field order and generating null values of missing fields
   */
  @NonNull private List<Schema.Column> columns;

  /** To delegate to correct cursor handler to get next page */
  private final CursorType type = CursorType.DEFAULT;

  /**
   * Truncate the @see DataRows to respect LIMIT clause and/or to identify last page to close scroll
   * context. docsLeft is decremented by fetch_size for call to get page of result.
   */
  private long rowsLeft;

  /**
   * @see org.opensearch.sql.legacy.executor.format.SelectResultSet
   */
  @NonNull private Map<String, String> fieldAliasMap;

  /** To get next batch of result */
  private String scrollId;

  /** To get Point In Time */
  private String pitId;

  /** To get next batch of result with search after api */
  private SearchSourceBuilder searchSourceBuilder;

  /** To get last sort values * */
  private Object[] sortFields;

  /** To reduce the number of rows left by fetchSize */
  @NonNull private Integer fetchSize;

  private Integer limit;

  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder}
   * from DSL query string.
   */
  private static final NamedXContentRegistry xContentRegistry =
      new NamedXContentRegistry(
          new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());

  @Override
  public CursorType getType() {
    return type;
  }

  @Override
  public String generateCursorId() {
    if (rowsLeft <= 0 || isCursorIdNullOrEmpty()) {
      return null;
    }
    JSONObject json = new JSONObject();
    json.put(FETCH_SIZE, fetchSize);
    json.put(ROWS_LEFT, rowsLeft);
    json.put(INDEX_PATTERN, indexPattern);
    json.put(SCHEMA_COLUMNS, getSchemaAsJson());
    json.put(FIELD_ALIAS_MAP, fieldAliasMap);
    if (LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      json.put(PIT_ID, pitId);
      String sortFieldValue =
          AccessController.doPrivileged(
              (PrivilegedAction<String>)
                  () -> {
                    try {
                      return objectMapper.writeValueAsString(sortFields);
                    } catch (JsonProcessingException e) {
                      throw new RuntimeException(
                          "Failed to parse sort fields from JSON string.", e);
                    }
                  });
      json.put(SORT_FIELDS, sortFieldValue);
      setSearchRequestString(json, searchSourceBuilder);
    } else {
      json.put(SCROLL_ID, scrollId);
    }
    return String.format("%s:%s", type.getId(), encodeCursor(json));
  }

  private void setSearchRequestString(JSONObject cursorJson, SearchSourceBuilder sourceBuilder) {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      XContentBuilder builder = XContentFactory.jsonBuilder(outputStream);
      sourceBuilder.toXContent(builder, null);
      builder.close();

      String searchRequestBase64 = Base64.getEncoder().encodeToString(outputStream.toByteArray());
      cursorJson.put("searchSourceBuilder", searchRequestBase64);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to set search request string on cursor json.", ex);
    }
  }

  private boolean isCursorIdNullOrEmpty() {
    return LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)
        ? Strings.isNullOrEmpty(pitId)
        : Strings.isNullOrEmpty(scrollId);
  }

  public static DefaultCursor from(String cursorId) {
    /**
     * It is assumed that cursorId here is the second part of the original cursor passed by the
     * client after removing first part which identifies cursor type
     */
    JSONObject json = decodeCursor(cursorId);
    DefaultCursor cursor = new DefaultCursor();
    cursor.setFetchSize(json.getInt(FETCH_SIZE));
    cursor.setRowsLeft(json.getLong(ROWS_LEFT));
    cursor.setIndexPattern(json.getString(INDEX_PATTERN));
    if (LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      populateCursorForPit(json, cursor);
    } else {
      cursor.setScrollId(json.getString(SCROLL_ID));
    }
    cursor.setColumns(getColumnsFromSchema(json.getJSONArray(SCHEMA_COLUMNS)));
    cursor.setFieldAliasMap(fieldAliasMap(json.getJSONObject(FIELD_ALIAS_MAP)));

    return cursor;
  }

  private static void populateCursorForPit(JSONObject json, DefaultCursor cursor) {
    cursor.setPitId(json.getString(PIT_ID));

    cursor.setSortFields(getSortFieldsFromJson(json));

    // Retrieve and set the SearchSourceBuilder from the JSON field
    String searchSourceBuilderBase64 = json.getString("searchSourceBuilder");
    byte[] bytes = Base64.getDecoder().decode(searchSourceBuilderBase64);
    ByteArrayInputStream streamInput = new ByteArrayInputStream(bytes);
    try {
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(xContentRegistry, IGNORE_DEPRECATIONS, streamInput);
      SearchSourceBuilder sourceBuilder = SearchSourceBuilder.fromXContent(parser);
      cursor.setSearchSourceBuilder(sourceBuilder);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to get searchSourceBuilder from cursor Id", ex);
    }
  }

  private static Object[] getSortFieldsFromJson(JSONObject json) {
    return AccessController.doPrivileged(
        (PrivilegedAction<Object[]>)
            () -> {
              try {
                return objectMapper.readValue(json.getString(SORT_FIELDS), Object[].class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to parse sort fields from JSON string.", e);
              }
            });
  }

  private JSONArray getSchemaAsJson() {
    JSONArray schemaJson = new JSONArray();

    for (Schema.Column column : columns) {
      schemaJson.put(schemaEntry(column.getName(), column.getAlias(), column.getType()));
    }

    return schemaJson;
  }

  private JSONObject schemaEntry(String name, String alias, String type) {
    JSONObject entry = new JSONObject();
    entry.put("name", name);
    if (alias != null) {
      entry.put("alias", alias);
    }
    entry.put("type", type);
    return entry;
  }

  private static String encodeCursor(JSONObject cursorJson) {
    return Base64.getEncoder().encodeToString(cursorJson.toString().getBytes());
  }

  private static JSONObject decodeCursor(String cursorId) {
    return new JSONObject(new String(Base64.getDecoder().decode(cursorId)));
  }

  private static Map<String, String> fieldAliasMap(JSONObject json) {
    Map<String, String> fieldToAliasMap = new HashMap<>();
    json.keySet().forEach(key -> fieldToAliasMap.put(key, json.get(key).toString()));
    return fieldToAliasMap;
  }

  private static List<Schema.Column> getColumnsFromSchema(JSONArray schema) {
    List<Schema.Column> columns =
        IntStream.range(0, schema.length())
            .mapToObj(
                i -> {
                  JSONObject jsonColumn = schema.getJSONObject(i);
                  return new Schema.Column(
                      jsonColumn.getString("name"),
                      jsonColumn.optString("alias", null),
                      Schema.Type.valueOf(jsonColumn.getString("type").toUpperCase()));
                })
            .collect(Collectors.toList());
    return columns;
  }
}
