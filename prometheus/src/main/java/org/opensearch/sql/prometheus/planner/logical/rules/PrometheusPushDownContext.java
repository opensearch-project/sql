/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical.rules;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * Accumulates pushdown state for Prometheus queries. Each pushdown rule adds state here, and the
 * physical scan materializes it into a PromQL query and HTTP request parameters.
 */
@Getter
public class PrometheusPushDownContext {

  /** Start time for the range query (epoch seconds). */
  @Setter private Long startTime;

  /** End time for the range query (epoch seconds). */
  @Setter private Long endTime;

  /** Step interval for the range query. */
  @Setter private String step;

  /** Label matchers for the metric selector (label_name -> label_value). */
  private final Map<String, String> labelMatchers;

  /** Whether a time range filter has been pushed down. */
  @Getter private boolean timeRangePushed;

  /** Whether a label filter has been pushed down. */
  @Getter private boolean labelFilterPushed;

  /** Default time range duration in seconds (1 hour). */
  private static final long DEFAULT_TIME_RANGE_SECONDS = 3600;

  /** Default step interval. */
  private static final String DEFAULT_STEP = "14";

  public PrometheusPushDownContext() {
    this.labelMatchers = new LinkedHashMap<>();
    this.timeRangePushed = false;
    this.labelFilterPushed = false;
    this.step = DEFAULT_STEP;
  }

  /** Copy constructor for creating independent copies during plan optimization. */
  private PrometheusPushDownContext(PrometheusPushDownContext other) {
    this.startTime = other.startTime;
    this.endTime = other.endTime;
    this.step = other.step;
    this.labelMatchers = new LinkedHashMap<>(other.labelMatchers);
    this.timeRangePushed = other.timeRangePushed;
    this.labelFilterPushed = other.labelFilterPushed;
  }

  /** Creates an independent copy of this context. */
  public PrometheusPushDownContext copy() {
    return new PrometheusPushDownContext(this);
  }

  /** Pushes a time range start boundary (>=, >). */
  public void pushStartTime(long epochSeconds) {
    this.startTime = epochSeconds;
    this.timeRangePushed = true;
  }

  /** Pushes a time range end boundary (<=, <). */
  public void pushEndTime(long epochSeconds) {
    this.endTime = epochSeconds;
    this.timeRangePushed = true;
  }

  /** Pushes a label equality matcher. */
  public void pushLabelMatcher(String labelName, String labelValue) {
    this.labelMatchers.put(labelName, labelValue);
    this.labelFilterPushed = true;
  }

  /** Gets the effective start time (defaults to now - 1 hour). */
  public long getEffectiveStartTime() {
    if (startTime != null) {
      return startTime;
    }
    return Instant.now().getEpochSecond() - DEFAULT_TIME_RANGE_SECONDS;
  }

  /** Gets the effective end time (defaults to now). */
  public long getEffectiveEndTime() {
    if (endTime != null) {
      return endTime;
    }
    return Instant.now().getEpochSecond();
  }

  /** Gets the effective step. */
  public String getEffectiveStep() {
    return step != null ? step : DEFAULT_STEP;
  }

  /**
   * Builds the PromQL metric selector string. For a metric named "up" with labels {job="node"},
   * returns: up{job="node"}
   */
  public String buildPromQL(String metricName) {
    if (labelMatchers.isEmpty()) {
      return metricName;
    }
    StringBuilder sb = new StringBuilder(metricName);
    sb.append("{");
    List<String> matchers = new ArrayList<>();
    for (Map.Entry<String, String> entry : labelMatchers.entrySet()) {
      matchers.add(entry.getKey() + "=\"" + entry.getValue() + "\"");
    }
    sb.append(String.join(",", matchers));
    sb.append("}");
    return sb.toString();
  }
}
