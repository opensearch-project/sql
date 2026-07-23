/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.opensearch.sql.calcite.SearchPredicateCompiler;

/** Runtime query-string input consumed by a correlated OpenSearch scan. */
public record DynamicQueryStringSpec(
    RexNode queryExpression,
    List<RexNode> queryParts,
    Set<Integer> runtimePredicateParts,
    SearchPredicateCompiler compiler) {

  /** Splits concatenation so only subsearch outputs are parsed as PPL predicates. */
  public static DynamicQueryStringSpec create(
      RexNode queryExpression, List<RexNode> runtimePredicates, SearchPredicateCompiler compiler) {
    List<RexNode> parts = new ArrayList<>();
    flattenConcatenation(queryExpression, parts);
    Set<Integer> predicateParts = new LinkedHashSet<>();
    for (int i = 0; i < parts.size(); i++) {
      RexNode part = parts.get(i);
      if (runtimePredicates.stream().anyMatch(predicate -> predicate == part)) {
        predicateParts.add(i);
      }
    }
    if (predicateParts.size() != runtimePredicates.size()) {
      throw new IllegalArgumentException(
          "Every runtime search predicate must be a query-string concatenation part");
    }
    return new DynamicQueryStringSpec(
        queryExpression, List.copyOf(parts), Set.copyOf(predicateParts), compiler);
  }

  /** Correlation variables referenced by the runtime query expression. */
  public Set<CorrelationId> correlationIds() {
    Set<CorrelationId> ids = new LinkedHashSet<>();
    for (RexNode queryPart : queryParts) {
      queryPart.accept(
          new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
              ids.add(correlVariable.id);
              return null;
            }
          });
    }
    return Set.copyOf(ids);
  }

  private static void flattenConcatenation(RexNode expression, List<RexNode> parts) {
    if (expression instanceof RexCall call
        && (call.getOperator().getName().equalsIgnoreCase("concat")
            || call.getOperator().getName().equals("||"))) {
      for (RexNode operand : call.getOperands()) {
        flattenConcatenation(operand, parts);
      }
      return;
    }
    parts.add(expression);
  }
}
