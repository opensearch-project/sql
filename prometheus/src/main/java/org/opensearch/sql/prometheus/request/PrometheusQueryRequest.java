/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Prometheus metric query request.
 */
@EqualsAndHashCode
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class PrometheusQueryRequest {

  /**
   * PromQL.
   */
  @Setter
  private String promQl;

  /**
   * startTime of the query.
   */
  @Setter
  private Long startTime;

  /**
   * endTime of the query.
   */
  @Setter
  private Long endTime;

  /**
   * step is the resolution required between startTime and endTime.
   */
  @Setter
  private String step;

}
