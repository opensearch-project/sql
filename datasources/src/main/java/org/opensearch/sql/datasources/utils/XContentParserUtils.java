/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.utils;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasource.model.DataSourceType;

/** Utitlity class to serialize and deserialize objects in XContent. */
@UtilityClass
public class XContentParserUtils {
  public static final String NAME_FIELD = "name";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String CONNECTOR_FIELD = "connector";
  public static final String PROPERTIES_FIELD = "properties";
  public static final String ALLOWED_ROLES_FIELD = "allowedRoles";

  public static final String RESULT_INDEX_FIELD = "resultIndex";
  public static final String STATUS_FIELD = "status";

  /**
   * Convert xcontent parser to DataSourceMetadata.
   *
   * @param parser parser.
   * @return DataSourceMetadata {@link DataSourceMetadata}
   * @throws IOException IOException.
   */
  public static DataSourceMetadata toDataSourceMetadata(XContentParser parser) throws IOException {
    String name = null;
    String description = StringUtils.EMPTY;
    DataSourceType connector = null;
    List<String> allowedRoles = new ArrayList<>();
    Map<String, String> properties = new HashMap<>();
    String resultIndex = null;
    DataSourceStatus status = null;
    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case NAME_FIELD:
          name = parser.textOrNull();
          break;
        case DESCRIPTION_FIELD:
          description = parser.textOrNull();
          break;
        case CONNECTOR_FIELD:
          connector = DataSourceType.fromString(parser.textOrNull());
          break;
        case ALLOWED_ROLES_FIELD:
          while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            allowedRoles.add(parser.text());
          }
          break;
        case PROPERTIES_FIELD:
          ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
          while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String key = parser.currentName();
            parser.nextToken();
            String value = parser.textOrNull();
            properties.put(key, value);
          }
          break;
        case RESULT_INDEX_FIELD:
          resultIndex = parser.textOrNull();
          break;
        case STATUS_FIELD:
          status = DataSourceStatus.fromString(parser.textOrNull());
          break;
        default:
          throw new IllegalArgumentException("Unknown field: " + fieldName);
      }
    }
    return new DataSourceMetadata.Builder()
        .setName(name)
        .setDescription(description)
        .setConnector(connector)
        .setProperties(properties)
        .setAllowedRoles(allowedRoles)
        .setResultIndex(resultIndex)
        .setDataSourceStatus(status)
        .validateAndBuild();
  }

  public static Map<String, Object> toMap(XContentParser parser) throws IOException {
    Map<String, Object> resultMap = new HashMap<>();
    String name;
    String description;
    List<String> allowedRoles = new ArrayList<>();
    Map<String, String> properties = new HashMap<>();
    String resultIndex;
    DataSourceStatus status;
    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case NAME_FIELD:
          name = parser.textOrNull();
          resultMap.put(NAME_FIELD, name);
          break;
        case DESCRIPTION_FIELD:
          description = parser.textOrNull();
          resultMap.put(DESCRIPTION_FIELD, description);
          break;
        case CONNECTOR_FIELD:
          // no-op - datasource connector should not be modified
          break;
        case ALLOWED_ROLES_FIELD:
          while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            allowedRoles.add(parser.text());
          }
          resultMap.put(ALLOWED_ROLES_FIELD, allowedRoles);
          break;
        case PROPERTIES_FIELD:
          ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
          while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String key = parser.currentName();
            parser.nextToken();
            String value = parser.textOrNull();
            properties.put(key, value);
          }
          resultMap.put(PROPERTIES_FIELD, properties);
          break;
        case RESULT_INDEX_FIELD:
          resultIndex = parser.textOrNull();
          resultMap.put(RESULT_INDEX_FIELD, resultIndex);
          break;
        case STATUS_FIELD:
          status = DataSourceStatus.fromString(parser.textOrNull());
          resultMap.put(STATUS_FIELD, status);
          break;
        default:
          throw new IllegalArgumentException("Unknown field: " + fieldName);
      }
    }
    if (resultMap.get(NAME_FIELD) == null || resultMap.get(NAME_FIELD) == "") {
      throw new IllegalArgumentException("Name is a required field.");
    }
    return resultMap;
  }

  /**
   * Converts json string to DataSourceMetadata.
   *
   * @param json jsonstring.
   * @return DataSourceMetadata {@link DataSourceMetadata}
   * @throws IOException IOException.
   */
  public static DataSourceMetadata toDataSourceMetadata(String json) throws IOException {
    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json)) {
      return toDataSourceMetadata(parser);
    }
  }

  /**
   * Converts json string to Map.
   *
   * @param json jsonstring.
   * @return DataSourceData
   * @throws IOException IOException.
   */
  public static Map<String, Object> toMap(String json) throws IOException {
    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json)) {
      return toMap(parser);
    }
  }

  /**
   * Converts DataSourceMetadata to XContentBuilder.
   *
   * @param metadata metadata.
   * @return XContentBuilder {@link XContentBuilder}
   * @throws Exception Exception.
   */
  public static XContentBuilder convertToXContent(DataSourceMetadata metadata) throws Exception {

    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    builder.field(NAME_FIELD, metadata.getName());
    builder.field(DESCRIPTION_FIELD, metadata.getDescription());
    builder.field(CONNECTOR_FIELD, metadata.getConnector().name());
    builder.field(ALLOWED_ROLES_FIELD, metadata.getAllowedRoles().toArray());
    builder.startObject(PROPERTIES_FIELD);
    for (Map.Entry<String, String> entry : metadata.getProperties().entrySet()) {
      builder.field(entry.getKey(), entry.getValue());
    }
    builder.endObject();
    builder.field(RESULT_INDEX_FIELD, metadata.getResultIndex());
    builder.field(STATUS_FIELD, metadata.getStatus());
    builder.endObject();
    return builder;
  }
}
