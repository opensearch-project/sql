/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.opensearch.sql.analysis.NestedAnalyzer.isNestedFunction;

import lombok.RequiredArgsConstructor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Lucene query that builds a native {@code exists} DSL fragment for {@code IS NULL} / {@code IS NOT
 * NULL} predicates.
 *
 * <p>This replaces the previous behavior of serializing these unary predicates as compounded script
 * queries. The native {@code exists} query is cheaper, AOSS / serverless compatible, and the
 * expected DSL shape downstream consumers look for.
 *
 * <p>Unlike most {@link LuceneQuery} subclasses this predicate family is unary (a single reference
 * argument) rather than the standard {ref, literal} pair, so this class overrides both {@link
 * #canSupport(FunctionExpression)} and {@link #build(FunctionExpression)}.
 *
 * <p>Nested-field predicates are intentionally NOT supported here: OpenSearch DSL does not handle
 * {@code IS_NULL} / {@code IS_NOT_NULL} on nested fields correctly (see the equivalent guard in
 * {@code PredicateAnalyzer} for the Calcite path). When the reference is a nested function, {@link
 * #canSupport} returns {@code false} and {@link
 * org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder} falls back to the script
 * query path, preserving correctness.
 */
@RequiredArgsConstructor
public class ExistsQuery extends LuceneQuery {

  /** When true, the predicate is {@code IS NULL} and the exists query is wrapped in must_not. */
  private final boolean negated;

  @Override
  public boolean canSupport(FunctionExpression func) {
    return func.getArguments().size() == 1
        && func.getArguments().get(0) instanceof ReferenceExpression
        && !isNestedFunction(func.getArguments().get(0));
  }

  /**
   * Unary IS NULL / IS NOT NULL has no {@code arg[1]}, so we must never route through {@link
   * org.opensearch.sql.opensearch.storage.script.filter.lucene.NestedQuery#buildNested} — that path
   * reads {@code func.getArguments().get(1)} and would throw. Returning {@code false} here forces
   * {@code FilterQueryBuilder} to fall back to the script-query path for nested-field predicates.
   */
  @Override
  public boolean isNestedPredicate(FunctionExpression func) {
    return false;
  }

  @Override
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
    String fieldName = ref.getRawPath();
    QueryBuilder existsQuery = QueryBuilders.existsQuery(fieldName);
    if (negated) {
      return QueryBuilders.boolQuery().mustNot(existsQuery);
    }
    return existsQuery;
  }
}
