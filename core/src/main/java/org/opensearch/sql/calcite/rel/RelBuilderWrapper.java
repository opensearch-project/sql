/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * RelBuilderWrapper delegates most public methods to the underlying RelBuilder while prohibiting
 * direct field access methods to maintain consistency with dynamic fields handling.
 */
@RequiredArgsConstructor
public class RelBuilderWrapper {
  private final RelBuilder relBuilder;

  public RelBuilder scan(String... tableNames) {
    return relBuilder.scan(tableNames);
  }

  public RelBuilder scan(Iterable<String> tableNames) {
    return relBuilder.scan(tableNames);
  }

  public RelBuilder values(RelDataType rowType, Object... values) {
    return relBuilder.values(rowType, values);
  }

  public RelBuilder values(String[] columnNames, Object... values) {
    return relBuilder.values(columnNames, values);
  }

  public RelBuilder filter(RexNode condition) {
    return relBuilder.filter(condition);
  }

  public RelBuilder filter(Iterable<? extends RexNode> conditions) {
    return relBuilder.filter(conditions);
  }

  public RelBuilder filter(Iterable<CorrelationId> variablesSet, RexNode... conditions) {
    return relBuilder.filter(variablesSet, conditions);
  }

  public RelBuilder project(Iterable<? extends RexNode> nodes) {
    return relBuilder.project(nodes);
  }

  public RelBuilder project(RexNode... nodes) {
    return relBuilder.project(nodes);
  }

  public RelBuilder project(Iterable<? extends RexNode> nodes, Iterable<String> fieldNames) {
    return relBuilder.project(nodes, fieldNames);
  }

  public RelBuilder project(
      Iterable<? extends RexNode> nodes, Iterable<String> fieldNames, boolean force) {
    return relBuilder.project(nodes, fieldNames, force);
  }

  public RelBuilder project(
      Iterable<? extends RexNode> nodes,
      Iterable<String> fieldNames,
      boolean force,
      Iterable<CorrelationId> variablesSet) {
    return relBuilder.project(nodes, fieldNames, force, variablesSet);
  }

  public RelBuilder projectPlus(RexNode... nodes) {
    return relBuilder.projectPlus(nodes);
  }

  public RelBuilder projectPlus(Iterable<RexNode> nodes) {
    return relBuilder.projectPlus(nodes);
  }

  public RelBuilder projectExcept(RexNode... nodes) {
    return relBuilder.projectExcept(nodes);
  }

  public RelBuilder projectExcept(Iterable<RexNode> nodes) {
    return relBuilder.projectExcept(nodes);
  }

  public RelBuilder rename(List<String> fieldNames) {
    return relBuilder.rename(fieldNames);
  }

  public RelBuilder as(String alias) {
    return relBuilder.as(alias);
  }

  public RelBuilder join(JoinRelType joinType, RexNode condition) {
    return relBuilder.join(joinType, condition);
  }

  public RelBuilder correlate(
      JoinRelType joinType, CorrelationId id, RexInputRef... requiredColumns) {
    return relBuilder.correlate(joinType, id, requiredColumns);
  }

  public RelBuilder correlate(
      JoinRelType joinType, CorrelationId id, Iterable<RexInputRef> requiredColumns) {
    return relBuilder.correlate(joinType, id, requiredColumns);
  }

  public RelBuilder union(boolean all) {
    return relBuilder.union(all);
  }

  public RelBuilder union(boolean all, int n) {
    return relBuilder.union(all, n);
  }

  public RelBuilder intersect(boolean all) {
    return relBuilder.intersect(all);
  }

  public RelBuilder intersect(boolean all, int n) {
    return relBuilder.intersect(all, n);
  }

  public RelBuilder minus(boolean all) {
    return relBuilder.minus(all);
  }

  public RelBuilder aggregate(RelBuilder.GroupKey groupKey, RelBuilder.AggCall... aggCalls) {
    return relBuilder.aggregate(groupKey, aggCalls);
  }

  public RelBuilder aggregate(RelBuilder.GroupKey groupKey, Iterable<RelBuilder.AggCall> aggCalls) {
    return relBuilder.aggregate(groupKey, aggCalls);
  }

  public RelBuilder aggregate(GroupKey groupKey, List<AggregateCall> aggregateCalls) {
    return relBuilder.aggregate(groupKey, aggregateCalls);
  }

  public RelBuilder sort(RexNode... nodes) {
    return relBuilder.sort(nodes);
  }

  public RelBuilder sort(Iterable<RexNode> nodes) {
    return relBuilder.sort(nodes);
  }

  public RelBuilder sortLimit(int offset, int fetch, RexNode... nodes) {
    return relBuilder.sortLimit(offset, fetch, nodes);
  }

  public RelBuilder sortLimit(int offset, int fetch, Iterable<RexNode> nodes) {
    return relBuilder.sortLimit(offset, fetch, nodes);
  }

  public RelBuilder limit(int offset, int fetch) {
    return relBuilder.limit(offset, fetch);
  }

  public RelBuilder distinct() {
    return relBuilder.distinct();
  }

  public RelBuilder uncollect(List<String> aliases, boolean withOrdinality) {
    return relBuilder.uncollect(aliases, withOrdinality);
  }

  public RelBuilder hints(RelHint... hints) {
    return relBuilder.hints(hints);
  }

  public RelBuilder hints(Iterable<RelHint> hints) {
    return relBuilder.hints(hints);
  }

  // Stack operations

  public RelBuilder push(RelNode node) {
    return relBuilder.push(node);
  }

  public RelNode peek() {
    return relBuilder.peek();
  }

  public RelNode peek(int n) {
    return relBuilder.peek(n);
  }

  public RelNode build() {
    return relBuilder.build();
  }

  public int size() {
    return relBuilder.size();
  }

  public void clear() {
    relBuilder.clear();
  }

  // RexNode creation methods

  public RexNode literal(Object value) {
    return relBuilder.literal(value);
  }

  public RexNode alias(RexNode expr, String alias) {
    return relBuilder.alias(expr, alias);
  }

  public RexNode cast(RexNode expr, SqlTypeName typeName) {
    return relBuilder.cast(expr, typeName);
  }

  public RexNode desc(RexNode node) {
    return relBuilder.desc(node);
  }

  public RexNode nullsFirst(RexNode node) {
    return relBuilder.nullsFirst(node);
  }

  public RexNode nullsLast(RexNode node) {
    return relBuilder.nullsLast(node);
  }

  // Comparison operations

  public RexNode equals(RexNode operand0, RexNode operand1) {
    return relBuilder.equals(operand0, operand1);
  }

  public RexNode notEquals(RexNode operand0, RexNode operand1) {
    return relBuilder.notEquals(operand0, operand1);
  }

  public RexNode lessThan(RexNode operand0, RexNode operand1) {
    return relBuilder.lessThan(operand0, operand1);
  }

  public RexNode lessThanOrEqual(RexNode operand0, RexNode operand1) {
    return relBuilder.lessThanOrEqual(operand0, operand1);
  }

  public RexNode greaterThan(RexNode operand0, RexNode operand1) {
    return relBuilder.greaterThan(operand0, operand1);
  }

  public RexNode greaterThanOrEqual(RexNode operand0, RexNode operand1) {
    return relBuilder.greaterThanOrEqual(operand0, operand1);
  }

  public RexNode isNull(RexNode operand) {
    return relBuilder.isNull(operand);
  }

  public RexNode isNotNull(RexNode operand) {
    return relBuilder.isNotNull(operand);
  }

  // Logical operations

  public RexNode and(RexNode... operands) {
    return relBuilder.and(operands);
  }

  public RexNode and(Iterable<? extends RexNode> operands) {
    return relBuilder.and(operands);
  }

  public RexNode or(RexNode... operands) {
    return relBuilder.or(operands);
  }

  public RexNode or(Iterable<? extends RexNode> operands) {
    return relBuilder.or(operands);
  }

  public RexNode not(RexNode operand) {
    return relBuilder.not(operand);
  }

  // Function calls

  public RexNode call(SqlOperator operator, RexNode... operands) {
    return relBuilder.call(operator, operands);
  }

  public RexNode call(SqlOperator operator, Iterable<? extends RexNode> operands) {
    return relBuilder.call(operator, operands);
  }

  // Aggregate functions

  public RelBuilder.AggCall aggregateCall(SqlAggFunction aggFunction, RexNode... operands) {
    return relBuilder.aggregateCall(aggFunction, operands);
  }

  public RelBuilder.AggCall aggregateCall(SqlAggFunction aggFunction, Iterable<RexNode> operands) {
    return relBuilder.aggregateCall(aggFunction, operands);
  }

  public AggCall count(RexNode... operands) {
    return relBuilder.count(operands);
  }

  public AggCall count(Iterable<? extends RexNode> operands) {
    return relBuilder.count(operands);
  }

  public AggCall count(boolean distinct, @Nullable String alias, RexNode... operands) {
    return relBuilder.count(distinct, alias, operands);
  }

  public AggCall count(
      boolean distinct, @Nullable String alias, Iterable<? extends RexNode> operands) {
    return relBuilder.count(distinct, alias, operands);
  }

  public RelBuilder.AggCall count() {
    return relBuilder.count();
  }

  public RelBuilder.AggCall count(boolean distinct, String alias, RexNode operand) {
    return relBuilder.count(distinct, alias, operand);
  }

  public RelBuilder.AggCall countStar(String alias) {
    return relBuilder.countStar(alias);
  }

  public RelBuilder.AggCall sum(boolean distinct, String alias, RexNode operand) {
    return relBuilder.sum(distinct, alias, operand);
  }

  public RelBuilder.AggCall sum(RexNode operand) {
    return relBuilder.sum(operand);
  }

  public RelBuilder.AggCall min(String alias, RexNode operand) {
    return relBuilder.min(alias, operand);
  }

  public RelBuilder.AggCall min(RexNode operand) {
    return relBuilder.min(operand);
  }

  public RelBuilder.AggCall max(String alias, RexNode operand) {
    return relBuilder.max(alias, operand);
  }

  public RelBuilder.AggCall max(RexNode operand) {
    return relBuilder.max(operand);
  }

  public RelBuilder.AggCall avg(boolean distinct, String alias, RexNode operand) {
    return relBuilder.avg(distinct, alias, operand);
  }

  public RelBuilder.AggCall avg(RexNode operand) {
    return relBuilder.avg(operand);
  }

  // GroupKey operations

  public RelBuilder.GroupKey groupKey(RexNode... nodes) {
    return relBuilder.groupKey(nodes);
  }

  public RelBuilder.GroupKey groupKey(Iterable<? extends RexNode> nodes) {
    return relBuilder.groupKey(nodes);
  }

  public RelBuilder.GroupKey groupKey(ImmutableBitSet groupSet) {
    return relBuilder.groupKey(groupSet);
  }

  public RelBuilder.GroupKey groupKey(
      ImmutableBitSet groupSet, Iterable<ImmutableBitSet> groupSets) {
    return relBuilder.groupKey(groupSet, groupSets);
  }

  // Variable operations

  public RelBuilder variable(java.util.function.Consumer<RexCorrelVariable> consumer) {
    return relBuilder.variable(consumer);
  }

  // Additional RelBuilder methods found in usage patterns

  public RexNode between(RexNode value, RexNode lowerBound, RexNode upperBound) {
    return relBuilder.between(value, lowerBound, upperBound);
  }

  public RexNode in(RelNode subqueryRel, Iterable<? extends RexNode> nodes) {
    return relBuilder.in(subqueryRel, nodes);
  }

  public RexNode scalarQuery(java.util.function.Function<RelBuilder, RelNode> fn) {
    return relBuilder.scalarQuery(fn);
  }

  public RexNode exists(java.util.function.Function<RelBuilder, RelNode> fn) {
    return relBuilder.exists(fn);
  }

  public org.apache.calcite.rex.RexBuilder getRexBuilder() {
    return relBuilder.getRexBuilder();
  }

  public org.apache.calcite.rel.type.RelDataTypeFactory getTypeFactory() {
    return relBuilder.getTypeFactory();
  }

  public RelFactories.TableScanFactory getScanFactory() {
    return relBuilder.getScanFactory();
  }

  // Additional arithmetic and utility methods
  public RexNode plus(RexNode operand0, RexNode operand1) {
    return relBuilder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS, operand0, operand1);
  }

  public RexNode minus(RexNode operand0, RexNode operand1) {
    return relBuilder.call(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS, operand0, operand1);
  }

  public RexNode multiply(RexNode operand0, RexNode operand1) {
    return relBuilder.call(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY, operand0, operand1);
  }

  public RexNode divide(RexNode operand0, RexNode operand1) {
    return relBuilder.call(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE, operand0, operand1);
  }

  public RexNode coalesce(RexNode... operands) {
    return relBuilder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.COALESCE, operands);
  }

  public RexNode coalesce(Iterable<RexNode> operands) {
    return relBuilder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.COALESCE, operands);
  }

  // Access to underlying RelBuilder for advanced operations
  public RelBuilder getRawRelBuilder() {
    return relBuilder;
  }

  public RelOptCluster getCluster() {
    return relBuilder.getCluster();
  }

  // Only ordinal-based field access is allowed
  public RexInputRef field(int fieldOrdinal) {
    return relBuilder.field(fieldOrdinal);
  }

  public RexInputRef field(int inputCount, int inputOrdinal, int fieldOrdinal) {
    return relBuilder.field(inputCount, inputOrdinal, fieldOrdinal);
  }

  public RexNode field(RexNode e, int ordinal) {
    return relBuilder.field(e, ordinal);
  }

  public ImmutableList<RexNode> fields(List<? extends Number> ordinals) {
    return relBuilder.fields(ordinals);
  }

  public ImmutableList<RexNode> fields(ImmutableBitSet ordinals) {
    return relBuilder.fields(ordinals);
  }

  // PROHIBITED FIELD ACCESS METHODS - These methods are not exposed to prevent field access by name

  // The following field access methods are intentionally not delegated:
  // - field(String fieldName)
  // - field(int inputCount, int inputOrdinal, String fieldName)
  // - field(String alias, String fieldName)
  // - field(int inputCount, String alias, String fieldName)
  // - field(RexNode e, String name)
  // - fields()
  // - fields(RelCollation collation)
  // - fields(Iterable<String> fieldNames)
  // - fields(Mappings.TargetMapping mapping)

  // Package-private field_ methods for internal use
  RexInputRef field_(String fieldName) {
    return relBuilder.field(1, 0, fieldName);
  }

  RexInputRef field_(int inputCount, int inputOrdinal, String fieldName) {
    return relBuilder.field(inputCount, inputOrdinal, fieldName);
  }

  RexInputRef field_(int inputCount, int inputOrdinal, int fieldOrdinal) {
    return relBuilder.field(inputCount, inputOrdinal, fieldOrdinal);
  }

  RexNode field_(String alias, String fieldName) {
    return relBuilder.field(alias, fieldName);
  }

  RexNode field_(int inputCount, String alias, String fieldName) {
    return relBuilder.field(inputCount, alias, fieldName);
  }

  RexNode field_(RexCorrelVariable correlVariable, String fieldName) {
    return relBuilder.field(correlVariable, fieldName);
  }
}
