/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.sql.prometheus.exception.PrometheusClientException;
import org.opensearch.sql.prometheus.model.MetricMetadata;

/*
 * @opensearch.experimental
 */
public class PrometheusClientImpl implements PrometheusClient {

  private static final Logger logger = LogManager.getLogger(PrometheusClientImpl.class);
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  private final OkHttpClient prometheusHttpClient;
  private final OkHttpClient alertmanagerHttpClient;

  private final URI prometheusUri;
  private final URI alertmanagerUri;
  private final URI rulerUri;

  public PrometheusClientImpl(OkHttpClient prometheusHttpClient, URI prometheusUri) {
    this(
        prometheusHttpClient,
        prometheusUri,
        prometheusHttpClient,
        URI.create(prometheusUri.toString().replaceAll("/$", "") + "/alertmanager"),
        prometheusUri);
  }

  public PrometheusClientImpl(
      OkHttpClient prometheusHttpClient,
      URI prometheusUri,
      OkHttpClient alertmanagerHttpClient,
      URI alertmanagerUri) {
    this(prometheusHttpClient, prometheusUri, alertmanagerHttpClient, alertmanagerUri,
        prometheusUri);
  }

  public PrometheusClientImpl(
      OkHttpClient prometheusHttpClient,
      URI prometheusUri,
      OkHttpClient alertmanagerHttpClient,
      URI alertmanagerUri,
      URI rulerUri) {
    this.prometheusHttpClient = prometheusHttpClient;
    this.prometheusUri = prometheusUri;
    this.alertmanagerHttpClient = alertmanagerHttpClient;
    this.alertmanagerUri = alertmanagerUri;
    this.rulerUri = rulerUri;
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
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());

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
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());

    logger.info("Received Prometheus response for instant query: code={}", response);
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONObject("data");
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
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());
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
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());
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
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());
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
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());
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
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONArray("data");
  }

  @Override
  public JSONObject getAlerts() throws IOException {
    String queryUrl =
        String.format("%s/api/v1/alerts", prometheusUri.toString().replaceAll("/$", ""));
    logger.debug("Making Prometheus alerts request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = AccessController.doPrivilegedChecked(() -> this.prometheusHttpClient.newCall(request).execute());
    JSONObject jsonObject = readResponse(response);
    return jsonObject.getJSONObject("data");
  }

  @Override
  public JSONObject getRules(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String queryUrl =
        String.format(
            "%s/api/v1/rules%s", prometheusUri.toString().replaceAll("/$", ""), queryString);
    logger.debug("Making Prometheus GET rules request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    try (Response response =
        AccessController.doPrivilegedChecked(
            () -> this.prometheusHttpClient.newCall(request).execute())) {
      String body = readRulerResponse(response, "GET all rules");
      return normalizeRulesResponse(body, null);
    }
  }

  @Override
  public JSONArray getAlertmanagerAlerts(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/alerts%s", baseUrl, queryString);

    logger.debug("Making Alertmanager alerts request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = AccessController.doPrivilegedChecked(() -> this.alertmanagerHttpClient.newCall(request).execute());

    return readAlertmanagerResponse(response);
  }

  @Override
  public JSONArray getAlertmanagerAlertGroups(Map<String, String> queryParams) throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/alerts/groups%s", baseUrl, queryString);

    logger.debug("Making Alertmanager alert groups request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = AccessController.doPrivilegedChecked(() -> this.alertmanagerHttpClient.newCall(request).execute());

    return readAlertmanagerResponse(response);
  }

  @Override
  public JSONArray getAlertmanagerReceivers() throws IOException {
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/receivers", baseUrl);

    logger.debug("Making Alertmanager receivers request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = AccessController.doPrivilegedChecked(() -> this.alertmanagerHttpClient.newCall(request).execute());

    return readAlertmanagerResponse(response);
  }

  @Override
  public JSONArray getAlertmanagerSilences() throws IOException {
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/silences", baseUrl);

    logger.debug("Making Get Alertmanager silences request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    Response response = AccessController.doPrivilegedChecked(() -> this.alertmanagerHttpClient.newCall(request).execute());

    return readAlertmanagerResponse(response);
  }

  @Override
  public String createAlertmanagerSilences(String silenceJson) throws IOException {
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/silences", baseUrl);

    logger.debug("Making Create Alertmanager silences request: {}", queryUrl);
    Request request = new Request.Builder()
            .url(queryUrl)
            .header("Content-Type", "application/json")
            .post(RequestBody.create(silenceJson.getBytes(StandardCharsets.UTF_8)))
            .build();
    Response response = AccessController.doPrivilegedChecked(() -> this.alertmanagerHttpClient.newCall(request).execute());

    if (response.isSuccessful()) {
      return Objects.requireNonNull(response.body()).string();
    } else {
      String errorBody = response.body() != null ? response.body().string() : "No response body";
      logger.error(
              "create Alertmanager Silence request failed with code: {}, error body: {}", response.code(), errorBody);
      throw new PrometheusClientException(
              String.format(
                      "Alertmanager request failed with code: %s. Error details: %s",
                      response.code(), errorBody));
    }
  }

  @Override
  public String deleteAlertmanagerSilence(String silenceId) throws IOException {
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl =
        String.format(
            "%s/api/v2/silence/%s",
            baseUrl, URLEncoder.encode(silenceId, StandardCharsets.UTF_8));

    logger.debug("Making Delete Alertmanager silence request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).delete().build();
    try (Response response =
        AccessController.doPrivilegedChecked(
            () -> this.alertmanagerHttpClient.newCall(request).execute())) {
      if (response.isSuccessful()) {
        return "{\"status\":\"success\"}";
      } else {
        String errorBody =
            response.body() != null ? response.body().string() : "No response body";
        logger.error(
            "Delete Alertmanager Silence request failed with code: {}, error body: {}",
            response.code(),
            errorBody);
        throw new PrometheusClientException(
            String.format(
                "Alertmanager request failed with code: %s. Error details: %s",
                response.code(), errorBody));
      }
    }
  }

  @Override
  public JSONObject getAlertmanagerStatus() throws IOException {
    String baseUrl = alertmanagerUri.toString().replaceAll("/$", "");
    String queryUrl = String.format("%s/api/v2/status", baseUrl);

    logger.debug("Making Alertmanager status request: {}", queryUrl);
    Request request = new Request.Builder().url(queryUrl).build();
    try (Response response =
        AccessController.doPrivilegedChecked(
            () -> this.alertmanagerHttpClient.newCall(request).execute())) {
      if (response.isSuccessful()) {
        String bodyString = Objects.requireNonNull(response.body()).string();
        logger.debug("Alertmanager status response body: {}", bodyString);
        return new JSONObject(bodyString);
      } else {
        String errorBody =
            response.body() != null ? response.body().string() : "No response body";
        logger.error(
            "Alertmanager status request failed with code: {}, error body: {}",
            response.code(),
            errorBody);
        throw new PrometheusClientException(
            String.format(
                "Alertmanager request failed with code: %s. Error details: %s",
                response.code(), errorBody));
      }
    }
  }

  @Override
  public JSONObject getRulesByNamespace(String namespace, Map<String, String> queryParams)
      throws IOException {
    String queryString = this.paramsToQueryString(queryParams);
    String queryUrl =
        String.format(
            "%s/api/v1/rules/%s%s",
            rulerUri.toString().replaceAll("/$", ""),
            URLEncoder.encode(namespace, StandardCharsets.UTF_8),
            queryString);
    logger.debug("Making Ruler GET request for namespace");
    Request request = new Request.Builder().url(queryUrl).build();
    try (Response response =
        AccessController.doPrivilegedChecked(
            () -> this.prometheusHttpClient.newCall(request).execute())) {
      String body = readRulerResponse(response, "GET namespace " + namespace);
      return normalizeRulesResponse(body, namespace);
    }
  }

  @Override
  public String createOrUpdateRuleGroup(String namespace, String yamlBody) throws IOException {
    String queryUrl =
        String.format(
            "%s/api/v1/rules/%s",
            rulerUri.toString().replaceAll("/$", ""),
            URLEncoder.encode(namespace, StandardCharsets.UTF_8));
    logger.debug("Making Ruler POST request to create/update rule group");
    Request request =
        new Request.Builder()
            .url(queryUrl)
            .header("Content-Type", "application/yaml")
            .post(RequestBody.create(yamlBody.getBytes(StandardCharsets.UTF_8)))
            .build();
    try (Response response =
        AccessController.doPrivilegedChecked(
            () -> this.prometheusHttpClient.newCall(request).execute())) {
      String body = readRulerResponse(response, "POST create/update rule group");
      return body.isEmpty() ? "{\"status\":\"success\"}" : body;
    }
  }

  @Override
  public String deleteRuleNamespace(String namespace) throws IOException {
    String queryUrl =
        String.format(
            "%s/api/v1/rules/%s",
            rulerUri.toString().replaceAll("/$", ""),
            URLEncoder.encode(namespace, StandardCharsets.UTF_8));
    logger.debug("Making Ruler DELETE request for namespace");
    Request request = new Request.Builder().url(queryUrl).delete().build();
    try (Response response =
        AccessController.doPrivilegedChecked(
            () -> this.prometheusHttpClient.newCall(request).execute())) {
      String body = readRulerResponse(response, "DELETE namespace " + namespace);
      return body.isEmpty() ? "{\"status\":\"success\"}" : body;
    }
  }

  @Override
  public String deleteRuleGroup(String namespace, String groupName) throws IOException {
    String queryUrl =
        String.format(
            "%s/api/v1/rules/%s/%s",
            rulerUri.toString().replaceAll("/$", ""),
            URLEncoder.encode(namespace, StandardCharsets.UTF_8),
            URLEncoder.encode(groupName, StandardCharsets.UTF_8));
    logger.debug("Making Ruler DELETE request for group");
    Request request = new Request.Builder().url(queryUrl).delete().build();
    try (Response response =
        AccessController.doPrivilegedChecked(
            () -> this.prometheusHttpClient.newCall(request).execute())) {
      String body = readRulerResponse(response, "DELETE group " + groupName);
      return body.isEmpty() ? "{\"status\":\"success\"}" : body;
    }
  }

  /**
   * Reads a Ruler API response, returning the body string on success or throwing on failure.
   * Consolidates the error-handling pattern shared by all Ruler methods.
   *
   * @param response The HTTP response
   * @param operationDescription Description for log messages (e.g., "GET all rules")
   * @return The response body as a string (empty string if body is null)
   * @throws IOException If there is an issue reading the response
   */
  private String readRulerResponse(Response response, String operationDescription)
      throws IOException {
    if (response.isSuccessful()) {
      return Objects.requireNonNull(response.body(), "Ruler response body is null").string();
    } else {
      String errorBody = response.body() != null ? response.body().string() : "No response body";
      logger.error(
          "Ruler {} request failed with code: {}, error body: {}",
          operationDescription,
          response.code(),
          errorBody);
      throw new PrometheusClientException(
          String.format(
              "Ruler request failed with code: %s. Error details: %s",
              response.code(), errorBody));
    }
  }

  /**
   * Normalizes a raw rule response body into a consistent {"groups":[...]} JSONObject. Handles
   * three response formats:
   *
   * <ul>
   *   <li>Prometheus/AMP JSON: {"status":"success","data":{"groups":[...]}} - extracts data
   *   <li>Cortex/Thanos YAML (all rules): Map of namespace to list of rule groups
   *   <li>Cortex/Thanos YAML (single namespace): List of rule groups
   * </ul>
   *
   * @param body The raw response body string
   * @param namespace Optional namespace name used as the "file" field on groups from YAML
   *     single-namespace responses. When null, indicates the body may be a YAML map of namespaces.
   * @return JSONObject with {"groups":[...]} structure
   */
  @SuppressWarnings("unchecked")
  private JSONObject normalizeRulesResponse(String body, String namespace) {
    if (body.isEmpty()) {
      return new JSONObject().put("groups", new JSONArray());
    }

    // 1. Try Prometheus/AMP JSON format first
    try {
      JSONObject jsonObject = new JSONObject(body);
      if ("success".equals(jsonObject.optString("status")) && jsonObject.has("data")) {
        return jsonObject.getJSONObject("data");
      }
      if (jsonObject.has("groups")) {
        return jsonObject;
      }
    } catch (JSONException e) {
      // Not JSON — fall through to YAML parsing
    }

    // 2. Parse as YAML (Cortex/Thanos format)
    try {
      Object parsed = YAML_MAPPER.readValue(body, Object.class);
      JSONArray groupsArray = new JSONArray();
      addGroupsFromParsed(parsed, namespace, groupsArray);
      return new JSONObject().put("groups", groupsArray);
    } catch (Exception e) {
      logger.warn(
          "Failed to parse rules response body, returning empty groups: {}", e.getMessage());
      return new JSONObject().put("groups", new JSONArray());
    }
  }

  @SuppressWarnings("unchecked")
  private void addGroupsFromParsed(Object parsed, String namespace, JSONArray groupsArray) {
    if (parsed instanceof Map) {
      // All-namespaces format: Map<String, List<RuleGroup>>
      Map<String, Object> namespacesMap = (Map<String, Object>) parsed;
      for (Map.Entry<String, Object> entry : namespacesMap.entrySet()) {
        if (entry.getValue() instanceof List) {
          addGroupsFromParsed(entry.getValue(), entry.getKey(), groupsArray);
        }
      }
    } else if (parsed instanceof List) {
      // Single-namespace format: List<RuleGroup>
      List<Map<String, Object>> groups = (List<Map<String, Object>>) parsed;
      for (Map<String, Object> group : groups) {
        JSONObject groupObj = new JSONObject(group);
        if (namespace != null) {
          groupObj.put("file", namespace);
        }
        groupsArray.put(groupObj);
      }
    }
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
      throw new PrometheusClientException(
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
        throw new PrometheusClientException(
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
        throw new PrometheusClientException(errorMessage);
      }
    } else {
      String errorBody = response.body() != null ? response.body().string() : "No response body";
      logger.error(
          "Prometheus request failed with code: {}, error body: {}", response.code(), errorBody);
      throw new PrometheusClientException(
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
