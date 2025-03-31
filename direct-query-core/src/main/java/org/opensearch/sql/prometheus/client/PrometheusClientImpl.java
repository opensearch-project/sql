/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.sql.prometheus.model.MetricMetadata;

public class PrometheusClientImpl implements PrometheusClient {

  private static final Logger logger = LogManager.getLogger(PrometheusClientImpl.class);

  private final OkHttpClient prometheusHttpClient;
  private final OkHttpClient alertmanagerHttpClient;

  private final URI prometheusUri;
  private final URI alertmanagerUri;

  public PrometheusClientImpl(OkHttpClient prometheusHttpClient, URI prometheusUri) {
    this(
        prometheusHttpClient,
        prometheusUri,
        prometheusHttpClient,
        URI.create(prometheusUri.toString().replaceAll("/$", "") + "/alertmanager"));
  }

  public PrometheusClientImpl(
      OkHttpClient prometheusHttpClient,
      URI prometheusUri,
      OkHttpClient alertmanagerHttpClient,
      URI alertmanagerUri) {
    this.prometheusHttpClient = prometheusHttpClient;
    this.prometheusUri = prometheusUri;
    this.alertmanagerHttpClient = alertmanagerHttpClient;
    this.alertmanagerUri = alertmanagerUri;
  }

  private String paramsToQueryString(Map<String, String> queryParams) {
    String queryString =
        queryParams.entrySet().stream()
            .map(
                entry ->
                    URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                        + "="
                        + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
            .collect(Collectors.joining("&"));
    return queryString.isEmpty() ? "" : "?" + queryString;
  }

  @Override
  public JSONObject queryRange(String query, Long start, Long end, String step) throws IOException {
    return queryRange(query, start, end, step, null, null);
  }

  @Override
  public JSONObject queryRange(
      String query, Long start, Long end, String step, Integer limit, Integer timeout)
      throws IOException {
    String queryString = buildQueryString(query, start, end, step, limit, timeout);
    String queryUrl =
        String.format(
            "%s/api/v1/query_range%s", prometheusUri.toString().replaceAll("/$", ""), queryString);

    logger.debug("Making Prometheus query_range request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();

    logger.debug("Executing Prometheus request with headers: {}", request.headers().toString());
    Response response = this.prometheusHttpClient.newCall(request).execute();

    logger.debug("Received Prometheus response for query_range: code={}", response);

    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONObject("data");
  }

  @Override
  public JSONObject query(String query, Long time, Integer limit, Integer timeout)
      throws IOException {
    Map<String, String> params = new HashMap<>();
    params.put("query", query);

    // Add optional parameters
    if (time != null) {
      params.put("time", time.toString());
    }
    if (limit != null) {
      params.put("limit", limit.toString());
    }
    if (timeout != null) {
      params.put("timeout", timeout.toString());
    }

    String queryString = this.paramsToQueryString(params);

    String queryUrl =
        String.format(
            "%s/api/v1/query%s", prometheusUri.toString().replaceAll("/$", ""), queryString);

    logger.info("Making Prometheus instant query request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();

    logger.info("Executing Prometheus request with headers: {}", request.headers().toString());
    Response response = this.prometheusHttpClient.newCall(request).execute();

    logger.info("Received Prometheus response for instant query: code={}", response);
    // Return the full response object, not just the data field
    return readResponse(response);
  }

  @Override
  public List<String> getLabels(String metricName) throws IOException {
    return getLabels(Map.of("match[]", metricName));
  }

  @Override
  public List<String> getLabels(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String queryUrl =
        String.format(
            "%s/api/v1/labels%s", prometheusUri.toString().replaceAll("/$", ""), queryString);
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.prometheusHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return toListOfLabels(jsonObject.getJSONArray("data"));
  }

  @Override
  public List<String> getLabel(String labelName, Map<String, String> queryParams)
      throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String queryUrl =
        String.format(
            "%s/api/v1/label/%s/values%s",
            prometheusUri.toString().replaceAll("/$", ""), labelName, queryString);
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.prometheusHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return toListOfLabels(jsonObject.getJSONArray("data"));
  }

  @Override
  public Map<String, List<MetricMetadata>> getAllMetrics(Map<String, String> queryParams)
      throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String queryUrl =
        String.format(
            "%s/api/v1/metadata%s", prometheusUri.toString().replaceAll("/$", ""), queryString);
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.prometheusHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    TypeReference<HashMap<String, List<MetricMetadata>>> typeRef = new TypeReference<>() {};
    return new ObjectMapper().readValue(jsonObject.getJSONObject("data").toString(), typeRef);
  }

  @Override
  public Map<String, List<MetricMetadata>> getAllMetrics() throws IOException {
    return getAllMetrics(Map.of());
  }

  @Override
  public List<Map<String, String>> getSeries(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String queryUrl =
        String.format(
            "%s/api/v1/series%s", prometheusUri.toString().replaceAll("/$", ""), queryString);
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.prometheusHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    JSONArray dataArray = jsonObject.getJSONArray("data");
    return toListOfSeries(dataArray);
  }

  @Override
  public JSONArray queryExemplars(String query, Long start, Long end) throws IOException {
    String queryUrl =
        String.format(
            "%s/api/v1/query_exemplars?query=%s&start=%s&end=%s",
            prometheusUri.toString().replaceAll("/$", ""),
            URLEncoder.encode(query, StandardCharsets.UTF_8),
            start,
            end);
    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.prometheusHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONArray("data");
  }

  @Override
  public JSONObject getAlerts() throws IOException {
    String queryUrl =
        String.format("%s/api/v1/alerts", prometheusUri.toString().replaceAll("/$", ""));
    logger.debug("Making Prometheus alerts request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.prometheusHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONObject("data");
  }

  @Override
  public JSONObject getRules(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String queryUrl =
        String.format(
            "%s/api/v1/rules%s", prometheusUri.toString().replaceAll("/$", ""), queryString);
    logger.debug("Making Prometheus rules request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.prometheusHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONObject("data");
  }

  @Override
  public JSONArray getAlertmanagerAlerts(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/alerts%s", baseUrl, queryString);

    logger.debug("Making Alertmanager alerts request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.alertmanagerHttpClient.newCall(request).execute();

    return readAlertmanagerResponse(response);
  }

  @Override
  public JSONArray getAlertmanagerAlertGroups(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/alerts/groups%s", baseUrl, queryString);

    logger.debug("Making Alertmanager alert groups request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.alertmanagerHttpClient.newCall(request).execute();

    return readAlertmanagerResponse(response);
  }

  @Override
  public JSONArray getAlertmanagerReceivers() throws IOException {
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/receivers", baseUrl);

    logger.debug("Making Alertmanager receivers request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.alertmanagerHttpClient.newCall(request).execute();

    return readAlertmanagerResponse(response);
  }

  @Override
  public JSONArray getAlertmanagerSilences() throws IOException {
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/silences", baseUrl);

    logger.debug("Making Alertmanager silences request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = this.alertmanagerHttpClient.newCall(request).execute();

    return readAlertmanagerResponse(response);
  }

  /**
   * Reads and processes an Alertmanager API response.
   *
   * @param response The HTTP response from Alertmanager
   * @return A JSONArray with the processed response
   * @throws IOException If there's an error reading the response
   */
  private JSONArray readAlertmanagerResponse(Response response) throws IOException {
    if (response.isSuccessful()) {
      String bodyString = Objects.requireNonNull(response.body()).string();
      logger.debug("Alertmanager response body: {}", bodyString);

      // Parse the response body directly as JSON array
      return new JSONArray(bodyString);
    } else {
      String errorBody = response.body() != null ? response.body().string() : "No response body";
      logger.error(
          "Alertmanager request failed with code: {}, error body: {}", response.code(), errorBody);
      throw new org.opensearch.sql.prometheus.exception.PrometheusClientException(
          String.format(
              "Alertmanager request failed with code: %s. Error details: %s",
              response.code(), errorBody));
    }
  }

  private List<String> toListOfLabels(JSONArray array) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      // __name__ is internal label in prometheus representing the metric name.
      // Exempting this from labels list as it is not required in any of the operations.
      if (!"__name__".equals(array.optString(i))) {
        result.add(array.optString(i));
      }
    }
    return result;
  }

  private List<Map<String, String>> toListOfSeries(JSONArray array) {
    List<Map<String, String>> result = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      Map<String, String> map = new HashMap<>();
      for (String key : obj.keySet()) {
        map.put(key, obj.getString(key));
      }
      result.add(map);
    }
    return result;
  }

  private JSONObject readResponse(Response response) throws IOException {
    // Log the response information
    logger.debug("Prometheus response code: {}", response.code());

    String requestId = response.header("X-Request-ID");
    if (requestId != null) {
      logger.info("Prometheus request ID: {}", requestId);
    }
    if (response.isSuccessful()) {
      JSONObject jsonObject;
      try {
        String bodyString = Objects.requireNonNull(response.body()).string();
        logger.debug("Prometheus response body: {}", bodyString);
        jsonObject = new JSONObject(bodyString);
      } catch (JSONException jsonException) {
        logger.error("Failed to parse Prometheus response as JSON", jsonException);
        throw new org.opensearch.sql.prometheus.exception.PrometheusClientException(
            "Prometheus returned unexpected body, "
                + "please verify your prometheus server setup.");
      }

      String status = jsonObject.getString("status");
      logger.debug("Prometheus response status: {}", status);

      if ("success".equals(status)) {
        return jsonObject;
      } else {
        String errorMessage = jsonObject.getString("error");
        logger.error("Prometheus returned error status: {}", errorMessage);
        throw new org.opensearch.sql.prometheus.exception.PrometheusClientException(errorMessage);
      }
    } else {
      String errorBody = response.body() != null ? response.body().string() : "No response body";
      logger.error(
          "Prometheus request failed with code: {}, error body: {}", response.code(), errorBody);
      throw new org.opensearch.sql.prometheus.exception.PrometheusClientException(
          String.format(
              "Request to Prometheus is Unsuccessful with code: %s. Error details: %s",
              response.code(), errorBody));
    }
  }

  private String buildQueryString(
      String query, Long start, Long end, String step, Integer limit, Integer timeout) {
    Map<String, String> params = new HashMap<>();
    params.put("query", query);
    params.put("start", start.toString());
    params.put("end", end.toString());
    params.put("step", step);

    if (limit != null) {
      params.put("limit", limit.toString());
    }
    if (timeout != null) {
      params.put("timeout", timeout.toString());
    }

    return this.paramsToQueryString(params);
  }
}
