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
import org.opensearch.sql.prometheus.data.value.PrometheusExprValueFactory;

/**
 * OpenSearch search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation. 2)
 * Indicate the search already done.
 */
@EqualsAndHashCode
@Getter
@ToString
public class PrometheusQueryRequest {

  /**
   * Default query timeout in minutes.
   */
  public static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);

  private final String metricName;

  /**
   * Prometheus Query.
   */
  private final StringBuilder prometheusQueryBuilder;

  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final PrometheusExprValueFactory exprValueFactory;

  @Getter
  @Setter
  private Long startTime;

  @Getter
  @Setter
  private Long endTime;

  @Getter
  @Setter
  private String step;

  /**
   * Constructor of ElasticsearchQueryRequest.
   */
  public PrometheusQueryRequest(String metricName,
                                PrometheusExprValueFactory factory) {
    this.metricName = metricName;
    this.prometheusQueryBuilder = new StringBuilder();
    this.exprValueFactory = factory;
  }
}
