/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.request;

import static org.opensearch.sql.doctest.core.request.SqlRequestFormat.OPENSEARCH_DASHBOARD_REQUEST;

import java.io.IOException;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.sql.doctest.core.response.SqlResponse;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Request to SQL plugin to isolate OpenSearch native request
 */
public class SqlRequest {

  public static final SqlRequest NONE = null;

  /**
   * Native OpenSearch request object
   */
  private final Request request;

  public SqlRequest(String method, String endpoint, String body, UrlParam... params) {
    this.request = makeRequest(method, endpoint, body, params);
  }

  /**
   * Send request to OpenSearch via client and create response for it.
   *
   * @param client restful client connection
   * @return sql response
   */
  public SqlResponse send(RestClient client) {
    try {
      return new SqlResponse(client.performRequest(request));
    } catch (IOException e) {
      // Some test may expect failure
      if (e instanceof ResponseException) {
        return new SqlResponse(((ResponseException) e).getResponse());
      }

      throw new IllegalStateException(StringUtils.format(
          "Exception occurred during sending request %s", OPENSEARCH_DASHBOARD_REQUEST.format(this)), e);
    }
  }

  /**
   * Expose request for request formatter.
   *
   * @return native OpenSearch format
   */
  public Request request() {
    return request;
  }

  private Request makeRequest(String method, String endpoint, String body, UrlParam[] params) {
    Request request = new Request(method, endpoint);
    request.setJsonEntity(body);
    for (UrlParam param : params) {
      request.addParameter(param.key, param.value);
    }

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  public static class UrlParam {
    private String key;
    private String value;

    public UrlParam(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public UrlParam(String keyValue) {
      int equality = keyValue.indexOf('=');
      if (equality == -1) {
        throw new IllegalArgumentException(String.format(
            "Key value pair is in bad format [%s]", keyValue));
      }

      this.key = keyValue.substring(0, equality);
      this.value = keyValue.substring(equality + 1);
    }
  }

}
