/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.calcite.SearchPredicateCompiler;
import org.opensearch.sql.exception.SemanticCheckException;

/** Runtime query-string input consumed by a correlated OpenSearch scan. */
public final class DynamicQueryStringSpec {

  private static final String ASSEMBLY_ERROR =
      "The search command could not apply the subsearch result.";

  private final RexNode queryExpression;
  private final List<RexNode> queryParts;
  private final Set<Integer> runtimePredicateParts;
  private final SearchPredicateCompiler compiler;

  public DynamicQueryStringSpec(
      RexNode queryExpression,
      List<RexNode> queryParts,
      Set<Integer> runtimePredicateParts,
      SearchPredicateCompiler compiler) {
    this.queryExpression = Objects.requireNonNull(queryExpression, ASSEMBLY_ERROR);
    this.queryParts = List.copyOf(queryParts);
    this.runtimePredicateParts = Set.copyOf(runtimePredicateParts);
    this.compiler = Objects.requireNonNull(compiler, ASSEMBLY_ERROR);
    validateCharacterParts(this.queryParts);
  }

  /** Splits concatenation so only subsearch outputs are parsed as PPL predicates. */
  public static DynamicQueryStringSpec create(
      RexNode queryExpression, List<RexNode> runtimePredicates, SearchPredicateCompiler compiler) {
    List<RexNode> parts = new ArrayList<>();
    flattenConcatenation(queryExpression, parts);
    Set<Integer> predicateParts = new LinkedHashSet<>();
    for (RexNode runtimePredicate : runtimePredicates) {
      boolean found = false;
      for (int i = 0; i < parts.size(); i++) {
        if (runtimePredicate == parts.get(i)) {
          predicateParts.add(i);
          found = true;
        }
      }
      if (!found) {
        throw new IllegalStateException(ASSEMBLY_ERROR);
      }
    }
    return new DynamicQueryStringSpec(
        queryExpression, List.copyOf(parts), Set.copyOf(predicateParts), compiler);
  }

  public RexNode queryExpression() {
    return queryExpression;
  }

  public List<RexNode> queryParts() {
    return queryParts;
  }

  public Set<Integer> runtimePredicateParts() {
    return runtimePredicateParts;
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

  /** Compiles runtime predicate parts and joins them with the static query-string parts. */
  public String buildRuntimeQuery(String[] values) {
    Objects.requireNonNull(values, ASSEMBLY_ERROR);
    if (values.length != queryParts.size()) {
      throw new IllegalStateException(ASSEMBLY_ERROR);
    }

    StringBuilder query = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      String value = values[i];
      if (runtimePredicateParts.contains(i)) {
        value = compiler.compile(value);
      } else if (value == null) {
        throw new IllegalStateException(ASSEMBLY_ERROR);
      }
      query.append(value);
    }
    return query.toString();
  }

  private static void validateCharacterParts(List<RexNode> parts) {
    for (RexNode part : parts) {
      if (!SqlTypeUtil.isCharacter(part.getType())) {
        throw new SemanticCheckException(
            "The search command can only use text produced by a subsearch, but the subsearch"
                + " returned "
                + part.getType().getSqlTypeName()
                + ". Convert the value with tostring() before returning it.");
      }
    }
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
