/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.opensearch.core.xcontent.XContentParser;

@Data
@AllArgsConstructor
public class CreateAsyncQueryRequest {

  private String query;
  private LangType lang;

  public static CreateAsyncQueryRequest fromXContentParser(XContentParser parser)
      throws IOException {
    String query = null;
    LangType lang = null;
    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      String fieldName = parser.currentName();
      parser.nextToken();
      if (fieldName.equals("query")) {
        query = parser.textOrNull();
      } else if (fieldName.equals("lang")) {
        lang = LangType.fromString(parser.textOrNull());
      } else {
        throw new IllegalArgumentException("Unknown field: " + fieldName);
      }
    }
    if (lang == null || query == null) {
      throw new IllegalArgumentException("lang and query are required fields.");
    }
    return new CreateAsyncQueryRequest(query, lang);
  }
}
