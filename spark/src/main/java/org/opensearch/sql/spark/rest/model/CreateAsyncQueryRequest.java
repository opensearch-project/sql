/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.sql.spark.execution.session.SessionModel.SESSION_ID;

import java.io.IOException;
import lombok.Data;
import org.apache.commons.lang3.Validate;
import org.opensearch.core.xcontent.XContentParser;

@Data
public class CreateAsyncQueryRequest {

  private String query;
  private String datasource;
  private LangType lang;
  // optional sessionId
  private String sessionId;

  public CreateAsyncQueryRequest(String query, String datasource, LangType lang) {
    this.query = Validate.notNull(query, "Query can't be null");
    this.datasource = Validate.notNull(datasource, "Datasource can't be null");
    this.lang = Validate.notNull(lang, "lang can't be null");
  }

  public CreateAsyncQueryRequest(String query, String datasource, LangType lang, String sessionId) {
    this.query = Validate.notNull(query, "Query can't be null");
    this.datasource = Validate.notNull(datasource, "Datasource can't be null");
    this.lang = Validate.notNull(lang, "lang can't be null");
    this.sessionId = sessionId;
  }

  public static CreateAsyncQueryRequest fromXContentParser(XContentParser parser)
      throws IOException {
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
        } else if (fieldName.equals(SESSION_ID)) {
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
