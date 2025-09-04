/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.format;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.util.Map;
import lombok.experimental.UtilityClass;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.spark.rest.model.LangType;

@UtilityClass
public class DirectQueryRequestConverter {

  public static ExecuteDirectQueryRequest fromXContentParser(XContentParser parser)
      throws Exception {
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    Map<String, Object> options = null;

    try {
      ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        String fieldName = parser.currentName();
        parser.nextToken();
        switch (fieldName) {
          case "datasource":
            request.setDataSources(parser.textOrNull());
            break;
          case "query":
            request.setQuery(parser.textOrNull());
            break;
          case "sessionId":
            request.setSessionId(parser.textOrNull());
            break;
          case "language":
            String language = parser.textOrNull();
            if (language != null) {
              try {
                request.setLanguage(LangType.fromString(language));
              } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid languageType: " + language);
              }
            }
            break;
          case "sourceVersion":
            request.setSourceVersion(parser.textOrNull());
            break;
          case "maxResults":
            request.setMaxResults(parser.intValue());
            break;
          case "timeout":
            request.setTimeout(parser.intValue());
            break;
          case "options":
            options = parser.map();
            break;
          default:
            parser.skipChildren();
        }
      }

      // Create the appropriate DataSourceOptions based on language type
      if (request.getLanguage() == LangType.PROMQL) {
        request.setOptions(createPrometheusOptions(options));
      }

      return request;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Error while parsing the direct query request: %s", e.getMessage()), e);
    }
  }

  /**
   * Creates and configures a PrometheusOptions object from the provided options map.
   *
   * @param options Map containing Prometheus specific options
   * @return Configured PrometheusOptions object
   */
  private static PrometheusOptions createPrometheusOptions(Map<String, Object> options) {
    PrometheusOptions prometheusOptions = new PrometheusOptions();
    if (options != null) {
      if (options.containsKey("queryType")) {
        prometheusOptions.setQueryType(
            PrometheusQueryType.fromString((String) options.get("queryType")));
      }
      if (options.containsKey("step")) {
        prometheusOptions.setStep((String) options.get("step"));
      }
      if (options.containsKey("time")) {
        prometheusOptions.setTime((String) options.get("time"));
      }
      if (options.containsKey("start")) {
        prometheusOptions.setStart((String) options.get("start"));
      }
      if (options.containsKey("end")) {
        prometheusOptions.setEnd((String) options.get("end"));
      }
    }
    return prometheusOptions;
  }
}
