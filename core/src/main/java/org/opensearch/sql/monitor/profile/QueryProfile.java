/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import com.google.gson.annotations.SerializedName;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/** Immutable snapshot of query profiling metrics. */
@Getter
public final class QueryProfile {

  private final Summary summary;

  private final Map<String, Phase> phases;

  private final PlanNode plan;

  /**
   * Create a new query profile snapshot.
   *
   * @param totalTimeMillis total elapsed milliseconds for the query (rounded to two decimals)
   * @param phases metric values keyed by {@link MetricName}
   */
  public QueryProfile(double totalTimeMillis, Map<MetricName, Double> phases) {
    this(totalTimeMillis, phases, null);
  }

  /**
   * Create a new query profile snapshot.
   *
   * @param totalTimeMillis total elapsed milliseconds for the query (rounded to two decimals)
   * @param phases metric values keyed by {@link MetricName}
   * @param plan plan tree profiling output
   */
  public QueryProfile(double totalTimeMillis, Map<MetricName, Double> phases, PlanNode plan) {
    this.summary = new Summary(totalTimeMillis);
    this.phases = buildPhases(phases);
    this.plan = plan;
  }

  private Map<String, Phase> buildPhases(Map<MetricName, Double> phases) {
    Objects.requireNonNull(phases, "phases");
    Map<String, Phase> ordered = new LinkedHashMap<>(MetricName.values().length);
    for (MetricName metricName : MetricName.values()) {
      Double value = phases.getOrDefault(metricName, 0d);
      ordered.put(metricName.name().toLowerCase(Locale.ROOT), new Phase(value));
    }
    return ordered;
  }

  @Getter
  public static final class Summary {

    @SerializedName("total_time_ms")
    private final double totalTimeMillis;

    private Summary(double totalTimeMillis) {
      this.totalTimeMillis = totalTimeMillis;
    }
  }

  @Getter
  public static final class Phase {

    @SerializedName("time_ms")
    private final double timeMillis;

    private Phase(double timeMillis) {
      this.timeMillis = timeMillis;
    }
  }

  @Getter
  public static final class PlanNode {

    private final String node;

    @SerializedName("time_ms")
    private final double timeMillis;

    private final long rows;

    private final List<PlanNode> children;

    public PlanNode(String node, double timeMillis, long rows, List<PlanNode> children) {
      this.node = node;
      this.timeMillis = timeMillis;
      this.rows = rows;
      this.children = children;
    }
  }
}
