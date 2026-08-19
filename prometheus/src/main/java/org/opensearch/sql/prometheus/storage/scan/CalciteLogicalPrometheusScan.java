/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage.scan;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.prometheus.planner.logical.rules.PrometheusPushDownContext;
import org.opensearch.sql.prometheus.planner.logical.rules.PrometheusRules;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;

/**
 * Logical scan node for Prometheus metrics in the Calcite planner. Supports pushdown of time range
 * filters and label matchers via {@link PrometheusPushDownContext}.
 */
public class CalciteLogicalPrometheusScan extends TableScan {

  @Getter private final PrometheusMetricTable prometheusTable;
  @Getter private final PrometheusPushDownContext pushDownContext;
  @Getter private final RelDataType schema;

  public CalciteLogicalPrometheusScan(
      RelOptCluster cluster, RelOptTable table, PrometheusMetricTable prometheusTable) {
    this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        table,
        prometheusTable,
        table.getRowType(),
        new PrometheusPushDownContext());
  }

  public CalciteLogicalPrometheusScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      PrometheusMetricTable prometheusTable,
      RelDataType schema,
      PrometheusPushDownContext pushDownContext) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.prometheusTable = prometheusTable;
    this.schema = schema;
    this.pushDownContext = pushDownContext;
  }

  @Override
  public RelDataType deriveRowType() {
    return schema;
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    for (RelOptRule rule : PrometheusRules.PROMETHEUS_RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost baseCost = super.computeSelfCost(planner, mq);
    if (baseCost == null) {
      return null;
    }
    // Reduce cost when filters are pushed down to prefer pushed-down plans
    double factor = 1.0;
    if (pushDownContext.isTimeRangePushed()) {
      factor *= 0.5;
    }
    if (pushDownContext.isLabelFilterPushed()) {
      factor *= 0.7;
    }
    return baseCost.multiplyBy(factor);
  }

  /**
   * Attempts to push a filter condition into the Prometheus scan. Returns a new scan with the
   * pushed-down condition removed, or null if nothing could be pushed.
   *
   * <p>Handles: - Time range comparisons on @timestamp (>, >=, <, <=) - Label equality conditions
   * (label = 'value')
   */
  public RelNode pushDownFilter(RexNode condition) {
    List<String> fieldNames = getRowType().getFieldNames();
    PrometheusPushDownContext newContext = pushDownContext.copy();
    RexNode remaining = pushDownCondition(condition, fieldNames, newContext);

    // If nothing was pushed, return null to indicate no transformation
    if (!newContext.isTimeRangePushed() && !newContext.isLabelFilterPushed()) {
      // Check if anything new was pushed compared to the original context
      if (newContext.getLabelMatchers().equals(pushDownContext.getLabelMatchers())
          && java.util.Objects.equals(newContext.getStartTime(), pushDownContext.getStartTime())
          && java.util.Objects.equals(newContext.getEndTime(), pushDownContext.getEndTime())) {
        return null;
      }
    }

    CalciteLogicalPrometheusScan newScan =
        new CalciteLogicalPrometheusScan(
            getCluster(), getTraitSet(), table, prometheusTable, schema, newContext);

    if (remaining != null) {
      // Some conditions couldn't be pushed — keep them as a Filter on top
      return getCluster()
          .getPlanner()
          .getContext()
          .unwrap(org.apache.calcite.tools.RelBuilder.class)
          == null
          ? org.apache.calcite.rel.logical.LogicalFilter.create(newScan, remaining)
          : org.apache.calcite.rel.logical.LogicalFilter.create(newScan, remaining);
    }
    return newScan;
  }

  /**
   * Recursively analyzes a condition, pushing what can be pushed into the context and returning the
   * remaining condition (or null if fully pushed).
   */
  private RexNode pushDownCondition(
      RexNode condition, List<String> fieldNames, PrometheusPushDownContext context) {
    if (!(condition instanceof RexCall)) {
      return condition; // Can't push non-call expressions
    }

    RexCall call = (RexCall) condition;

    if (call.getKind() == SqlKind.AND) {
      // Process each AND operand independently
      List<RexNode> remaining = new java.util.ArrayList<>();
      for (RexNode operand : call.getOperands()) {
        RexNode r = pushDownCondition(operand, fieldNames, context);
        if (r != null) {
          remaining.add(r);
        }
      }
      if (remaining.isEmpty()) {
        return null;
      } else if (remaining.size() == 1) {
        return remaining.get(0);
      } else {
        return getCluster().getRexBuilder().makeCall(call.getOperator(), remaining);
      }
    }

    // Try to push this single condition
    if (tryPushTimeRange(call, fieldNames, context)) {
      return null; // Successfully pushed
    }
    if (tryPushLabelMatcher(call, fieldNames, context)) {
      return null; // Successfully pushed
    }

    return condition; // Can't push — return as remaining
  }

  /**
   * Tries to push a time range comparison (e.g., @timestamp >= '2024-01-01'). Returns true if
   * pushed.
   */
  private boolean tryPushTimeRange(
      RexCall call, List<String> fieldNames, PrometheusPushDownContext context) {
    SqlKind kind = call.getKind();
    if (kind != SqlKind.GREATER_THAN
        && kind != SqlKind.GREATER_THAN_OR_EQUAL
        && kind != SqlKind.LESS_THAN
        && kind != SqlKind.LESS_THAN_OR_EQUAL) {
      return false;
    }

    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    // Determine which side is the field reference and which is the literal
    String fieldName = null;
    RexNode literal = null;
    boolean fieldOnLeft = false;

    if (left instanceof RexInputRef && isTimestampLiteral(right)) {
      fieldName = fieldNames.get(((RexInputRef) left).getIndex());
      literal = right;
      fieldOnLeft = true;
    } else if (right instanceof RexInputRef && isTimestampLiteral(left)) {
      fieldName = fieldNames.get(((RexInputRef) right).getIndex());
      literal = left;
      fieldOnLeft = false;
    }

    if (fieldName == null || !fieldName.equals("@timestamp")) {
      return false;
    }

    long epochSeconds = extractEpochSeconds(literal);
    // Determine effective comparison direction
    SqlKind effectiveKind = fieldOnLeft ? kind : reverseComparison(kind);

    switch (effectiveKind) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        context.pushStartTime(epochSeconds);
        return true;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        context.pushEndTime(epochSeconds);
        return true;
      default:
        return false;
    }
  }

  /** Tries to push a label equality condition (e.g., job = 'prometheus'). Returns true if pushed. */
  private boolean tryPushLabelMatcher(
      RexCall call, List<String> fieldNames, PrometheusPushDownContext context) {
    if (call.getKind() != SqlKind.EQUALS) {
      return false;
    }

    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    String fieldName = null;
    String value = null;

    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      fieldName = fieldNames.get(((RexInputRef) left).getIndex());
      value = extractStringLiteral((RexLiteral) right);
    } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
      fieldName = fieldNames.get(((RexInputRef) right).getIndex());
      value = extractStringLiteral((RexLiteral) left);
    }

    if (fieldName == null || value == null) {
      return false;
    }

    // Don't push @timestamp or @value equality — those aren't label selectors
    if (fieldName.equals("@timestamp") || fieldName.equals("@value")) {
      return false;
    }

    context.pushLabelMatcher(fieldName, value);
    return true;
  }

  private boolean isTimestampLiteral(RexNode node) {
    if (node instanceof RexLiteral) {
      return true; // Will try to extract epoch seconds
    }
    // Handle CAST expressions (e.g., CAST('2024-01-01' AS TIMESTAMP))
    if (node instanceof RexCall) {
      RexCall castCall = (RexCall) node;
      if (castCall.getKind() == SqlKind.CAST || castCall.getKind() == SqlKind.REINTERPRET) {
        return castCall.getOperands().get(0) instanceof RexLiteral;
      }
    }
    return false;
  }

  private long extractEpochSeconds(RexNode node) {
    if (node instanceof RexLiteral) {
      RexLiteral lit = (RexLiteral) node;
      SqlTypeName typeName = lit.getType().getSqlTypeName();
      if (typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        // Calcite stores timestamps as millis since epoch
        Long millis = lit.getValueAs(Long.class);
        return millis != null ? millis / 1000 : 0;
      } else if (typeName == SqlTypeName.BIGINT || typeName == SqlTypeName.INTEGER) {
        Long val = lit.getValueAs(Long.class);
        return val != null ? val : 0;
      }
      // Try generic number extraction
      Number num = lit.getValueAs(Number.class);
      return num != null ? num.longValue() : 0;
    }
    if (node instanceof RexCall) {
      RexCall castCall = (RexCall) node;
      return extractEpochSeconds(castCall.getOperands().get(0));
    }
    return 0;
  }

  private String extractStringLiteral(RexLiteral literal) {
    if (literal.getType().getSqlTypeName() == SqlTypeName.CHAR
        || literal.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
      return literal.getValueAs(String.class);
    }
    return null;
  }

  private SqlKind reverseComparison(SqlKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      default:
        return kind;
    }
  }
}
