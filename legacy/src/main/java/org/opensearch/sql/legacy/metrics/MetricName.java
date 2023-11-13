/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public enum MetricName {
  REQ_TOTAL("request_total"),
  REQ_COUNT_TOTAL("request_count"),
  FAILED_REQ_COUNT_SYS("failed_request_count_syserr"),
  FAILED_REQ_COUNT_CUS("failed_request_count_cuserr"),
  FAILED_REQ_COUNT_CB("failed_request_count_cb"),
  DEFAULT_CURSOR_REQUEST_TOTAL("default_cursor_request_total"),
  DEFAULT_CURSOR_REQUEST_COUNT_TOTAL("default_cursor_request_count"),
  CIRCUIT_BREAKER("circuit_breaker"),
  DEFAULT("default"),

  PPL_REQ_TOTAL("ppl_request_total"),
  PPL_REQ_COUNT_TOTAL("ppl_request_count"),
  PPL_FAILED_REQ_COUNT_SYS("ppl_failed_request_count_syserr"),
  PPL_FAILED_REQ_COUNT_CUS("ppl_failed_request_count_cuserr"),
  DATASOURCE_REQ_COUNT("datasource_request_count"),
  DATASOURCE_FAILED_REQ_COUNT_SYS("datasource_failed_request_count_syserr"),
  DATASOURCE_FAILED_REQ_COUNT_CUS("datasource_failed_request_count_cuserr");

  private String name;

  MetricName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static List<String> getNames() {
    return Arrays.stream(MetricName.values()).map(v -> v.name).collect(Collectors.toList());
  }

  private static Set<MetricName> NUMERICAL_METRIC =
      new ImmutableSet.Builder<MetricName>()
          .add(PPL_REQ_TOTAL)
          .add(PPL_REQ_COUNT_TOTAL)
          .add(PPL_FAILED_REQ_COUNT_SYS)
          .add(PPL_FAILED_REQ_COUNT_CUS)
          .build();

  public boolean isNumerical() {
    return this == REQ_TOTAL
        || this == REQ_COUNT_TOTAL
        || this == FAILED_REQ_COUNT_SYS
        || this == FAILED_REQ_COUNT_CUS
        || this == FAILED_REQ_COUNT_CB
        || this == DEFAULT
        || this == DEFAULT_CURSOR_REQUEST_TOTAL
        || this == DEFAULT_CURSOR_REQUEST_COUNT_TOTAL
        || NUMERICAL_METRIC.contains(this);
  }
}
