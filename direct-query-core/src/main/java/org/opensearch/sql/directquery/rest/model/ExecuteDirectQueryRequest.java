/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.opensearch.sql.directquery.model.DataSourceOptions;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.spark.rest.model.LangType;

@Data
@NoArgsConstructor
public class ExecuteDirectQueryRequest {
  // Required fields
  private String dataSources; // Required: From URI path parameter or request body
  private String query; // Required: String for Prometheus, object for CloudWatch
  @Setter private LangType language; // Required: SQL, PPL, or PROMQL
  private String sourceVersion; // Required: API version

  // Optional fields
  private Integer maxResults; // Optional: limit for Prometheus, maxDataPoints for CW
  private Integer timeout; // Optional: number of seconds
  private DataSourceOptions options; // Optional: Source specific arguments
  private String sessionId; // For session management

  /**
   * Helper method to get PrometheusOptions. If options is already PrometheusOptions, returns it.
   * Otherwise, returns a new empty PrometheusOptions.
   *
   * @return PrometheusOptions object
   */
  @NonNull
  public PrometheusOptions getPrometheusOptions() {
    if (options instanceof PrometheusOptions) {
      return (PrometheusOptions) options;
    }

    // Create new PrometheusOptions if options is null or not PrometheusOptions
    return new PrometheusOptions();
  }

  /**
   * Set Prometheus options.
   *
   * @param prometheusOptions The Prometheus options
   */
  public void setPrometheusOptions(PrometheusOptions prometheusOptions) {
    this.options = prometheusOptions;
  }
}
