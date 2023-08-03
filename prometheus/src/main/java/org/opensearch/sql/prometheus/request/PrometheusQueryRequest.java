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
public class PrometheusQueryRequest {

  /**
   * PromQL.
   */
  private String promQl;

  /**
   * startTime of the query.
   */
  private Long startTime;

  /**
   * endTime of the query.
   */
  private Long endTime;

  /**
   * step is the resolution required between startTime and endTime.
   */
  private String step;

}
