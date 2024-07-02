/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.format;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import lombok.experimental.UtilityClass;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.LangType;

@UtilityClass
public class CreateAsyncQueryRequestConverter {
  public static CreateAsyncQueryRequest fromXContentParser(XContentParser parser) {
    String query = null;
    LangType lang = null;
    String datasource = null;
    String sessionId = null;
    try {
      ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        String fieldName = parser.currentName();
        parser.nextToken();
        if (fieldName.equals("query")) {
          query = parser.textOrNull();
        } else if (fieldName.equals("lang")) {
          String langString = parser.textOrNull();
          lang = LangType.fromString(langString);
        } else if (fieldName.equals("datasource")) {
          datasource = parser.textOrNull();
        } else if (fieldName.equals("sessionId")) {
          sessionId = parser.textOrNull();
        } else {
          throw new IllegalArgumentException("Unknown field: " + fieldName);
        }
      }
      return new CreateAsyncQueryRequest(query, datasource, lang, sessionId);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Error while parsing the request body: %s", e.getMessage()));
    }
  }
}
