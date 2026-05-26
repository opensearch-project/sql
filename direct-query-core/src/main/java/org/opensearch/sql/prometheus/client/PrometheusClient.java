/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.datasource.client.DataSourceClient;
import org.opensearch.sql.prometheus.model.MetricMetadata;

/*
 * @opensearch.experimental
 */
public interface PrometheusClient extends DataSourceClient {

  JSONObject queryRange(String query, Long start, Long end, String step) throws IOException;

  JSONObject queryRange(
      String query, Long start, Long end, String step, Integer limit, Integer timeout)
      throws IOException;

  List<String> getLabels(String metricName) throws IOException;

  List<String> getLabels(Map<String, String> queryParams) throws IOException;

  List<String> getLabel(String labelName, Map<String, String> queryParams) throws IOException;

  Map<String, List<MetricMetadata>> getAllMetrics() throws IOException;

  Map<String, List<MetricMetadata>> getAllMetrics(Map<String, String> queryParams)
      throws IOException;

  List<Map<String, String>> getSeries(Map<String, String> queryParams) throws IOException;

  JSONArray queryExemplars(String query, Long start, Long end) throws IOException;

  /**
   * Execute an instant query at a single point in time.
   *
   * @param query The Prometheus expression query string
   * @param time Optional evaluation timestamp (Unix timestamp in seconds)
   * @return JSONObject containing the query result data
   * @throws IOException If there is an issue with the request
   */
  JSONObject query(String query, Long time, Integer limit, Integer timeout) throws IOException;

  /**
   * Get all alerting rules.
   *
   * @return JSONObject containing the alerting rules
   * @throws IOException If there is an issue with the request
   */
  JSONObject getAlerts() throws IOException;

  /**
   * Get all recording and alerting rules, normalized to a consistent JSON format. Handles
   * Prometheus JSON, Cortex/Thanos YAML, and AMP JSON responses, returning them all as a
   * {"groups":[...]} structure.
   *
   * @param queryParams Map of query parameters to include in the request
   * @return JSONObject with {"groups":[...]} structure
   * @throws IOException If there is an issue with the request
   */
  JSONObject getRules(Map<String, String> queryParams) throws IOException;

  /**
   * Get all alerts from Alertmanager.
   *
   * @param queryParams Map of query parameters to include in the request
   * @return JSONArray containing the alerts
   * @throws IOException If there is an issue with the request
   */
  JSONArray getAlertmanagerAlerts(Map<String, String> queryParams) throws IOException;

  /**
   * Get alerts grouped according to Alertmanager configuration.
   *
   * @param queryParams Map of query parameters to include in the request
   * @return JSONArray containing the alert groups
   * @throws IOException If there is an issue with the request
   */
  JSONArray getAlertmanagerAlertGroups(Map<String, String> queryParams) throws IOException;

  /**
   * Get all receivers configured in Alertmanager.
   *
   * @return JSONArray containing the receivers
   * @throws IOException If there is an issue with the request
   */
  JSONArray getAlertmanagerReceivers() throws IOException;

  /**
   * Get all silences configured in Alertmanager.
   *
   * @return JSONArray containing the silences
   * @throws IOException If there is an issue with the request
   */
  JSONArray getAlertmanagerSilences() throws IOException;

  /**
   * Creates a silence in Alertmanager.
   *
   * @return String containing the create silence response
   * @throws IOException If there is an issue with the request
   */
  String createAlertmanagerSilences(String silenceJson) throws IOException;

  /**
   * Expire (delete) a silence in Alertmanager.
   *
   * @param silenceId The ID of the silence to expire
   * @return String containing the response
   * @throws IOException If there is an issue with the request
   */
  String deleteAlertmanagerSilence(String silenceId) throws IOException;

  /**
   * Get Alertmanager status including configuration, version, and cluster info.
   *
   * @return JSONObject containing the Alertmanager status
   * @throws IOException If there is an issue with the request
   */
  JSONObject getAlertmanagerStatus() throws IOException;

  /**
   * Get rules for a specific namespace, normalized to a consistent JSON format. Handles
   * Cortex/Thanos YAML and AMP JSON responses, returning them all as a {"groups":[...]} structure.
   *
   * @param namespace The rules namespace
   * @param queryParams Map of query parameters to include in the request
   * @return JSONObject with {"groups":[...]} structure
   * @throws IOException If there is an issue with the request
   */
  JSONObject getRulesByNamespace(String namespace, Map<String, String> queryParams)
      throws IOException;

  /**
   * Create or update a rule group in a namespace via the Cortex/Thanos Ruler API.
   *
   * @param namespace The rules namespace
   * @param yamlBody The rule group definition in YAML format
   * @return String containing the response
   * @throws IOException If there is an issue with the request
   */
  String createOrUpdateRuleGroup(String namespace, String yamlBody) throws IOException;

  /**
   * Delete all rules in a namespace via the Cortex/Thanos Ruler API.
   *
   * @param namespace The rules namespace to delete
   * @return String containing the response
   * @throws IOException If there is an issue with the request
   */
  String deleteRuleNamespace(String namespace) throws IOException;

  /**
   * Delete a specific rule group in a namespace via the Cortex/Thanos Ruler API.
   *
   * @param namespace The rules namespace
   * @param groupName The specific rule group name to delete
   * @return String containing the response
   * @throws IOException If there is an issue with the request
   */
  String deleteRuleGroup(String namespace, String groupName) throws IOException;
}
