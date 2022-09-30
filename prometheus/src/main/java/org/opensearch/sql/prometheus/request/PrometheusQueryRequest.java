/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.common.unit.TimeValue;

/**
 * Prometheus metric query request.
 */
@EqualsAndHashCode
@Getter
@ToString
public class PrometheusQueryRequest {

  public static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);

  /**
   * PromQL.
   */
  private final StringBuilder promQl;

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

  /**
   * Constructor of PrometheusQueryRequest.
   */
  public PrometheusQueryRequest() {
    this.promQl = new StringBuilder();
  }
}
