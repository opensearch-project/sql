/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Prometheus metric query request.
 */
@EqualsAndHashCode
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusQueryExemplarsRequest {

  /**
   * PromQL.
   */
  private String query;

  /**
   * startTime of the query.
   */
  private Long startTime;

  /**
   * endTime of the query.
   */
  private Long endTime;

}
