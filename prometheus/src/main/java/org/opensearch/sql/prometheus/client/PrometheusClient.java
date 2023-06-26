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
import org.opensearch.sql.prometheus.request.system.model.MetricMetadata;

public interface PrometheusClient {

  JSONObject queryRange(String query, Long start, Long end, String step) throws IOException;

  List<String> getLabels(String metricName) throws IOException;

  Map<String, List<MetricMetadata>> getAllMetrics() throws IOException;

  JSONArray queryExemplars(String query, Long start, Long end) throws IOException;
}
