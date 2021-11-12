/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.request;

import static java.util.stream.Collectors.joining;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.http.Header;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.utils.JsonPrettyFormatter;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Different SQL request formats.
 */
public enum SqlRequestFormat {

  IGNORE_REQUEST {
    @Override
    public String format(SqlRequest request) {
      return "";
    }
  },
  CURL_REQUEST {
    @Override
    public String format(SqlRequest sqlRequest) {
      Request request = sqlRequest.request();
      StringBuilder str = new StringBuilder();
      str.append(">> curl ");

      List<Header> headers = request.getOptions().getHeaders();
      if (!headers.isEmpty()) {
        str.append(headers.stream().
            map(header -> StringUtils.format("-H '%s: %s'", header.getName(), header.getValue())).
            collect(joining(" ", "", " ")));
      }

      str.append(StringUtils.format("-X %s ", request.getMethod())).
          append("localhost:9200").append(request.getEndpoint());

      if (!request.getParameters().isEmpty()) {
        str.append(formatParams(request.getParameters()));
      }

      String body = body(request);
      if (!body.isEmpty()) {
        str.append(" -d '").
            append(body).
            append('\'');
      }
      return str.toString();
    }
  },
  OPENSEARCH_DASHBOARD_REQUEST {
    @Override
    public String format(SqlRequest sqlRequest) {
      Request request = sqlRequest.request();
      StringBuilder str = new StringBuilder();
      str.append(request.getMethod()).
          append(" ").
          append(request.getEndpoint());

      if (!request.getParameters().isEmpty()) {
        str.append(formatParams(request.getParameters()));
      }

      str.append('\n').
          append(body(request));
      return str.toString();
    }
  };

  /**
   * Format SQL request to specific format for documentation.
   *
   * @param request sql request
   * @return string in specific format
   */
  public abstract String format(SqlRequest request);

  @SuppressWarnings("UnstableApiUsage")
  protected String body(Request request) {
    String body = "";
    try {
      InputStream content = request.getEntity().getContent();
      String rawBody = CharStreams.toString(new InputStreamReader(content, Charsets.UTF_8));
      if (!rawBody.isEmpty()) {
        JSONObject json = new JSONObject(rawBody);
        String sql = json.optString("query"); // '\\n' in literal is replaced by '\n' after unquote
        body = JsonPrettyFormatter.format(rawBody);

        // Format and replace multi-line sql literal
        if (!sql.isEmpty() && sql.contains("\n")) {
          String multiLineSql =
              Arrays.stream(sql.split("\\n")). // '\\n' is to escape backslash in regex
                  collect(joining("\n\t",
                  "\"\"\"\n\t",
                  "\n\t\"\"\""));
          body = body.replace("\"" + sql.replace("\n", "\\n") + "\"", multiLineSql);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse and format body from request", e);
    }
    return body;
  }

  protected String formatParams(Map<String, String> params) {
    return params.entrySet().stream().
        map(e -> e.getKey() + "=" + e.getValue()).
        collect(joining("&", "?", ""));
  }
}
