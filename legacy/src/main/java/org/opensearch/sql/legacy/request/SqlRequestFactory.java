/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.rest.RestRequest;

public class SqlRequestFactory {

  private static final String SQL_URL_PARAM_KEY = "sql";
  private static final String SQL_FIELD_NAME = "query";
  private static final String PARAM_FIELD_NAME = "parameters";
  private static final String PARAM_TYPE_FIELD_NAME = "type";
  private static final String PARAM_VALUE_FIELD_NAME = "value";

  public static final String SQL_CURSOR_FIELD_NAME = "cursor";
  public static final String SQL_FETCH_FIELD_NAME = "fetch_size";

  public static SqlRequest getSqlRequest(RestRequest request) {
    switch (request.method()) {
      case POST:
        return parseSqlRequestFromPayload(request);
      default:
        throw new IllegalArgumentException(
            "OpenSearch SQL doesn't supported HTTP " + request.method().name());
    }
  }

  private static SqlRequest parseSqlRequestFromUrl(RestRequest restRequest) {
    String sql;

    sql = restRequest.param(SQL_URL_PARAM_KEY);
    if (sql == null) {
      throw new IllegalArgumentException("Cannot find sql parameter from the URL");
    }
    return new SqlRequest(sql, null);
  }

  private static SqlRequest parseSqlRequestFromPayload(RestRequest restRequest) {
    String content = restRequest.content().utf8ToString();

    JSONObject jsonContent;
    try {
      jsonContent = new JSONObject(content);
      if (jsonContent.has(SQL_CURSOR_FIELD_NAME)) {
        return new SqlRequest(jsonContent.getString(SQL_CURSOR_FIELD_NAME));
      }
    } catch (JSONException e) {
      throw new IllegalArgumentException("Failed to parse request payload", e);
    }
    String sql = jsonContent.getString(SQL_FIELD_NAME);

    if (jsonContent.has(PARAM_FIELD_NAME)) { // is a PreparedStatement
      JSONArray paramArray = jsonContent.getJSONArray(PARAM_FIELD_NAME);
      List<PreparedStatementRequest.PreparedStatementParameter> parameters =
          parseParameters(paramArray);
      return new PreparedStatementRequest(
          sql, validateAndGetFetchSize(jsonContent), jsonContent, parameters);
    }
    return new SqlRequest(sql, validateAndGetFetchSize(jsonContent), jsonContent);
  }

  private static Integer validateAndGetFetchSize(JSONObject jsonContent) {
    Optional<Integer> fetchSize = Optional.empty();
    try {
      if (jsonContent.has(SQL_FETCH_FIELD_NAME)) {
        fetchSize = Optional.of(jsonContent.getInt(SQL_FETCH_FIELD_NAME));
        if (fetchSize.get() < 0) {
          throw new IllegalArgumentException("Fetch_size must be greater or equal to 0");
        }
      }
    } catch (JSONException e) {
      throw new IllegalArgumentException("Failed to parse field [" + SQL_FETCH_FIELD_NAME + "]", e);
    }
    return fetchSize.orElse(0);
  }

  private static List<PreparedStatementRequest.PreparedStatementParameter> parseParameters(
      JSONArray paramsJsonArray) {
    List<PreparedStatementRequest.PreparedStatementParameter> parameters = new ArrayList<>();
    for (int i = 0; i < paramsJsonArray.length(); i++) {
      JSONObject paramJson = paramsJsonArray.getJSONObject(i);
      String typeString = paramJson.getString(PARAM_TYPE_FIELD_NAME);
      if (typeString == null) {
        throw new IllegalArgumentException(
            "Parameter type cannot be null. parameter json: " + paramJson.toString());
      }
      PreparedStatementRequest.ParameterType type;
      try {
        type = PreparedStatementRequest.ParameterType.valueOf(typeString.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported parameter type " + typeString, e);
      }
      try {
        PreparedStatementRequest.PreparedStatementParameter parameter;
        switch (type) {
          case BOOLEAN:
            parameter =
                new PreparedStatementRequest.PreparedStatementParameter<>(
                    paramJson.getBoolean(PARAM_VALUE_FIELD_NAME));
            parameters.add(parameter);
            break;
          case KEYWORD:
          case STRING:
          case DATE:
            parameter =
                new PreparedStatementRequest.StringParameter(
                    paramJson.getString(PARAM_VALUE_FIELD_NAME));
            parameters.add(parameter);
            break;
          case BYTE:
          case SHORT:
          case INTEGER:
          case LONG:
            parameter =
                new PreparedStatementRequest.PreparedStatementParameter<>(
                    paramJson.getLong(PARAM_VALUE_FIELD_NAME));
            parameters.add(parameter);
            break;
          case FLOAT:
          case DOUBLE:
            parameter =
                new PreparedStatementRequest.PreparedStatementParameter<>(
                    paramJson.getDouble(PARAM_VALUE_FIELD_NAME));
            parameters.add(parameter);
            break;
          case NULL:
            parameter = new PreparedStatementRequest.NullParameter();
            parameters.add(parameter);
            break;
          default:
            throw new IllegalArgumentException("Failed to handle parameter type " + type.name());
        }
      } catch (JSONException e) {
        throw new IllegalArgumentException("Failed to parse PreparedStatement parameters", e);
      }
    }
    return parameters;
  }
}
