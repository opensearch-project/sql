/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;

public interface PrometheusClient {

  JSONObject queryRange(String query, Long start, Long end, String step) throws IOException;

  List<String> getLabels(String metricName) throws IOException;
}