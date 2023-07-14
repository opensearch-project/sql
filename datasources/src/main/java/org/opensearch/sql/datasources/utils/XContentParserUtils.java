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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

/**
 * Utitlity class to serialize and deserialize objects in XContent.
 */
@UtilityClass
public class XContentParserUtils {
  public static final String NAME_FIELD = "name";
  public static final String CONNECTOR_FIELD = "connector";
  public static final String PROPERTIES_FIELD = "properties";
  public static final String ALLOWED_ROLES_FIELD = "allowedRoles";

  /**
   * Convert xcontent parser to DataSourceMetadata.
   *
   * @param parser parser.
   * @return DataSourceMetadata {@link DataSourceMetadata}
   * @throws IOException IOException.
   */
  public static DataSourceMetadata toDataSourceMetadata(XContentParser parser) throws IOException {
    String name = null;
    DataSourceType connector = null;
    List<String> allowedRoles = new ArrayList<>();
    Map<String, String> properties = new HashMap<>();
    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case NAME_FIELD:
          name = parser.textOrNull();
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
        default:
          throw new IllegalArgumentException("Unknown field: " + fieldName);
      }
    }
    if (name == null || connector == null) {
      throw new IllegalArgumentException("name and connector are required fields.");
    }
    return new DataSourceMetadata(name, connector, allowedRoles, properties);
  }

  /**
   * Converts json string to DataSourceMetadata.
   *
   * @param json jsonstring.
   * @return DataSourceMetadata {@link DataSourceMetadata}
   * @throws IOException IOException.
   */
  public static DataSourceMetadata toDataSourceMetadata(String json) throws IOException {
    try (XContentParser parser = XContentType.JSON.xContent()
        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            json)) {
      return toDataSourceMetadata(parser);
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
    builder.field(CONNECTOR_FIELD, metadata.getConnector().name());
    builder.field(ALLOWED_ROLES_FIELD, metadata.getAllowedRoles().toArray());
    builder.startObject(PROPERTIES_FIELD);
    for (Map.Entry<String, String> entry : metadata.getProperties().entrySet()) {
      builder.field(entry.getKey(), entry.getValue());
    }
    builder.endObject();
    builder.endObject();
    return builder;
  }


}
