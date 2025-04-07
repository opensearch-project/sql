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
   * Get all recording and alerting rules.
   *
   * @param queryParams Map of query parameters to include in the request
   * @return JSONObject containing all rules
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
}
