/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.sql.calcite.plan.DynamicQueryStringPushDown;
import org.opensearch.sql.exception.SemanticCheckException;

/** Rewrites runtime search scalar subqueries into a standard Calcite correlate. */
public final class RuntimeSearchCorrelator {

  private RuntimeSearchCorrelator() {}

  /**
   * Moves scalar subqueries out of a {@code query_string} filter and makes them the left input of a
   * correlate. The right scan consumes their single-row output through a correlation variable, so
   * its OpenSearch request is not built until every subquery result is available.
   */
  public static RelNode correlate(
      RelNode filterNode, SearchPredicateCompiler searchPredicateCompiler) {
    if (!(filterNode instanceof Filter filter)) {
      throw new IllegalStateException(
          "Runtime search query must produce a filter, but got "
              + filterNode.getClass().getSimpleName());
    }
    List<RexSubQuery> subqueries = findSubqueries(filter.getCondition());
    if (subqueries.isEmpty()) {
      return filter;
    }

    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    RelNode queryInput = combineSubqueries(subqueries, rexBuilder);
    CorrelationId correlationId = filter.getCluster().createCorrel();
    RexCorrelVariable correlation =
        (RexCorrelVariable) rexBuilder.makeCorrel(queryInput.getRowType(), correlationId);

    Map<RexSubQuery, RexNode> correlatedInputs = new IdentityHashMap<>();
    int fieldOffset = 0;
    for (RexSubQuery subquery : subqueries) {
      correlatedInputs.put(subquery, rexBuilder.makeFieldAccess(correlation, fieldOffset));
      fieldOffset += subquery.rel.getRowType().getFieldCount();
    }

    RexNode correlatedCondition =
        filter
            .getCondition()
            .accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitSubQuery(RexSubQuery subquery) {
                    RexNode correlatedInput = correlatedInputs.get(subquery);
                    return correlatedInput == null
                        ? super.visitSubQuery(subquery)
                        : correlatedInput;
                  }
                });

    RelNode right =
        pushDownDynamicQueryString(
            filter.getInput(),
            correlatedCondition,
            subqueries.stream().map(correlatedInputs::get).toList(),
            correlationId,
            searchPredicateCompiler);
    LogicalCorrelate correlate =
        LogicalCorrelate.create(
            queryInput,
            right,
            correlationId,
            ImmutableBitSet.range(queryInput.getRowType().getFieldCount()),
            JoinRelType.INNER);

    int leftFieldCount = queryInput.getRowType().getFieldCount();
    List<RexNode> parentFields =
        IntStream.range(0, filter.getRowType().getFieldCount())
            .mapToObj(index -> (RexNode) rexBuilder.makeInputRef(correlate, leftFieldCount + index))
            .toList();
    return LogicalProject.create(
        correlate, List.of(), parentFields, filter.getRowType().getFieldNames());
  }

  private static List<RexSubQuery> findSubqueries(RexNode condition) {
    List<RexSubQuery> subqueries = new ArrayList<>();
    Set<RexSubQuery> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    condition.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitSubQuery(RexSubQuery subquery) {
            if (seen.add(subquery)) {
              if (subquery.rel.getRowType().getFieldCount() != 1) {
                throw new SemanticCheckException(
                    "Implicit format subsearch must return exactly one column");
              }
              subqueries.add(subquery);
            }
            return null;
          }
        });
    return subqueries;
  }

  private static RelNode combineSubqueries(List<RexSubQuery> subqueries, RexBuilder rexBuilder) {
    RelNode result = subqueries.getFirst().rel;
    for (int i = 1; i < subqueries.size(); i++) {
      result =
          LogicalJoin.create(
              result,
              subqueries.get(i).rel,
              List.of(),
              rexBuilder.makeLiteral(true),
              Set.of(),
              JoinRelType.INNER);
    }
    return result;
  }

  private static RelNode pushDownDynamicQueryString(
      RelNode input,
      RexNode condition,
      List<RexNode> runtimePredicates,
      CorrelationId correlationId,
      SearchPredicateCompiler searchPredicateCompiler) {
    if (input instanceof DynamicQueryStringPushDown pushDown) {
      if (searchPredicateCompiler == null) {
        throw new IllegalStateException("No PPL search predicate compiler is configured");
      }
      return pushDown.pushDownDynamicQueryString(
          condition, runtimePredicates, searchPredicateCompiler);
    }
    return LogicalFilter.create(input, condition, ImmutableSet.of(correlationId));
  }
}
