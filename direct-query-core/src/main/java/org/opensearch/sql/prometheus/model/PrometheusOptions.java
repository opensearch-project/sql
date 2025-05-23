/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.opensearch.sql.directquery.model.DataSourceOptions;

/** Prometheus-specific options for direct queries. */
@Data
@NoArgsConstructor
public class PrometheusOptions implements DataSourceOptions {
  private PrometheusQueryType queryType;
  private String step; // Duration string in seconds
  private String time; // ISO timestamp for instant queries
  private String start; // ISO timestamp for range queries
  private String end; // ISO timestamp for range queries
}
