/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.response;

import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.client.Response;
import org.opensearch.sql.util.TestUtils;

/**
 * Response from SQL plugin
 */
public class SqlResponse {

  public static final SqlResponse NONE = null;

  /**
   * Native OpenSearch response
   */
  private final Response response;

  public SqlResponse(Response response) {
    this.response = response;
  }

  /**
   * Parse body in the response.
   *
   * @return response body
   */
  public String body() {
    try {
      return replaceChangingFields(TestUtils.getResponseBody(response, true));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read response body", e);
    }
  }

  /**
   * In OpenSearch response, there is field changed between each query, such as "took".
   * We have to replace those variants with fake constant to avoid re-generate documents.
   * The order of fields in JSON is a little different from original because of internal
   * key set in org.json.
   */
  private String replaceChangingFields(String response) {
    try {
      JSONObject root = new JSONObject(response);
      if (root.has("took")) {
        root.put("took", 100);
      } else {
        return response; // return original response to minimize impact
      }

      if (root.has("_shards")) {
        JSONObject shards = root.getJSONObject("_shards");
        shards.put("total", 5);
        shards.put("successful", 5);
      }
      return root.toString();
    } catch (JSONException e) {
      // Response is not a valid JSON which is not our interest.
      return response;
    }
  }
}
