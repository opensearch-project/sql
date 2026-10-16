/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.KnnStep;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * The {@code knn} command: {@code knn field=<field> vector=<vector> [k=<n>] [ef_search=<n>]
 * [nprobes=<n>]}. It keeps the rows of its input nearest to the query vector, at most k of them,
 * ordered by a new {@code _score} column. It means the same as the {@code knn(...)} predicate in
 * {@code where} ({@link org.opensearch.sql.ast.expression.KnnFunction}): {@code where p | knn ...}
 * is {@code where p and knn(...)}.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Knn extends UnresolvedPlan implements KnnStep {

  private UnresolvedPlan child;

  /** The {@code knn_vector} field to search. */
  private final Field field;

  /** The query vector: an {@code array(...)} of numeric literals. */
  private final UnresolvedExpression vector;

  /** Named options such as {@code k}, {@code ef_search} and {@code nprobes}. */
  private final List<Argument> options;

  /** Set by {@code KnnLimitResolver}; not part of the query text, so not part of equality. */
  @EqualsAndHashCode.Exclude private @Nullable ResolvedK resolvedK;

  /** Set by {@code KnnLimitResolver} when k cannot be fixed; not part of equality. */
  @EqualsAndHashCode.Exclude @ToString.Exclude private @Nullable SemanticCheckException unresolvedK;

  public Knn(Field field, UnresolvedExpression vector, List<Argument> options) {
    this.field = field;
    this.vector = vector;
    this.options = options;
  }

  @Override
  public void setResolvedK(ResolvedK resolvedK) {
    this.resolvedK = resolvedK;
    this.unresolvedK = null;
  }

  @Override
  public void setUnresolvedK(SemanticCheckException reason) {
    this.unresolvedK = reason;
    this.resolvedK = null;
  }

  @Override
  public Knn attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitKnn(this, context);
  }
}
