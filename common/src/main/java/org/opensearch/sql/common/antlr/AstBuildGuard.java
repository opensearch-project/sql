/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;

/**
 * Guards AST construction against pathological queries by bounding the depth of parse-tree visitor
 * recursion, rejecting over-nested expressions with an {@link IllegalArgumentException} instead of
 * letting a StackOverflowError crash the node.
 *
 * <p>Holds per-traversal state, so an instance must not be shared across threads.
 */
@RequiredArgsConstructor
public final class AstBuildGuard {

  public static final int DEFAULT_MAX_DEPTH = 1000;

  /** Maximum nesting depth allowed; {@code 0} or less means unlimited. */
  private final int maxDepth;

  /** Live nesting depth of the in-progress traversal; resets to 0 between top-level visits. */
  private int depth = 0;

  public AstBuildGuard() {
    this(DEFAULT_MAX_DEPTH);
  }

  /** Builds a guard from the configured {@code plugins.query.max_expression_depth} setting. */
  public static AstBuildGuard fromSettings(Settings settings) {
    Integer configured =
        settings == null ? null : settings.getSettingValue(Settings.Key.MAX_EXPRESSION_DEPTH);
    return new AstBuildGuard(configured == null ? DEFAULT_MAX_DEPTH : configured);
  }

  /**
   * Runs a single AST-build descent under the configured guardrails.
   *
   * @param visit the visitor descent to execute
   * @return the result of {@code visit}
   * @throws IllegalArgumentException if a positive maximum depth is configured and would be
   *     exceeded
   */
  public <T> T enforce(Supplier<T> visit) {
    if (maxDepth > 0 && depth >= maxDepth) {
      throw new IllegalArgumentException(
          StringUtils.format(
              "Expression nesting depth exceeds the maximum allowed [%d]; simplify the query or"
                  + " reduce the number of chained conditions.",
              maxDepth));
    }
    depth++;
    try {
      return visit.get();
    } finally {
      depth--;
    }
  }
}
