/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * A k-NN step of a query: the {@code knn} command ({@link org.opensearch.sql.ast.tree.Knn}) or the
 * {@code knn(...)} predicate in {@code where} ({@link
 * org.opensearch.sql.ast.expression.KnnFunction}). Both mean "the rows nearest to a query vector",
 * and both carry the number of neighbours, k, once the AST pass {@code KnnLimitResolver} has fixed
 * it.
 */
public interface KnnStep {

  /** Name of the command and of the function. */
  String NAME = "knn";

  /**
   * The k fixed by {@code KnnLimitResolver}.
   *
   * @return the resolved k, or {@code null} if the pass has not run on this node yet
   */
  @Nullable ResolvedK getResolvedK();

  /**
   * Record the k fixed by {@code KnnLimitResolver}; clears {@link #getUnresolvedK()}.
   *
   * @param resolvedK the resolved k
   */
  void setResolvedK(ResolvedK resolvedK);

  /**
   * Why {@code KnnLimitResolver} could not fix k, e.g. <i>missing k</i>. The lowering raises it
   * after checking the step's input, so that a misplaced step reports its placement first.
   *
   * @return the error, or {@code null} if k is resolved or the pass has not run on this node yet
   */
  @Nullable SemanticCheckException getUnresolvedK();

  /**
   * Record why {@code KnnLimitResolver} could not fix k; clears {@link #getResolvedK()}.
   *
   * @param reason the error to raise when the step is lowered
   */
  void setUnresolvedK(SemanticCheckException reason);

  /**
   * The number of nearest neighbours of a k-NN step.
   *
   * @param value k, in [1, 10000]
   * @param explicit {@code true} if the query sets {@code k=}, {@code false} if k was derived from
   *     a following {@code head}
   */
  record ResolvedK(int value, boolean explicit) {}
}
