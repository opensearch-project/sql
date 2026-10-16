/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.KnnStep;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * The {@code knn(<field>, <vector>, [k=<n>], ...)} predicate, a {@link Function} named {@code knn}
 * whose arguments are the {@code knn_vector} field, the query vector ({@code array(...)} of numeric
 * literals) and {@link UnresolvedArgument} options. It is only valid as a top-level conjunct of a
 * {@code where} condition; the other conjuncts are its pre-filter.
 *
 * <p>It differs from a plain {@code Function} only in carrying the k that {@code KnnLimitResolver}
 * resolves, which is not part of equality: it equals a {@code Function} with the same name and
 * arguments.
 */
public class KnnFunction extends Function implements KnnStep {

  private @Nullable ResolvedK resolvedK;
  private @Nullable SemanticCheckException unresolvedK;

  public KnnFunction(List<UnresolvedExpression> funcArgs) {
    super(NAME, funcArgs);
  }

  @Override
  public @Nullable ResolvedK getResolvedK() {
    return resolvedK;
  }

  @Override
  public void setResolvedK(ResolvedK resolvedK) {
    this.resolvedK = resolvedK;
    this.unresolvedK = null;
  }

  @Override
  public @Nullable SemanticCheckException getUnresolvedK() {
    return unresolvedK;
  }

  @Override
  public void setUnresolvedK(SemanticCheckException reason) {
    this.unresolvedK = reason;
    this.resolvedK = null;
  }
}
