/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node representing the PPL foreach command. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Foreach extends UnresolvedPlan {
  private final Mode mode;
  private final Map<String, String> options;
  private final List<String> fieldPatterns;
  private final UnresolvedExpression collectionExpression;
  private final List<ForeachEvalClause> evalClauses;
  private UnresolvedPlan child;

  @Override
  public Foreach attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return child == null ? ImmutableList.of() : ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitForeach(this, context);
  }

  /** Iteration mode of the foreach command. */
  public enum Mode {
    MULTIFIELD,
    MULTIVALUE,
    JSON_ARRAY,
    AUTO_COLLECTIONS;

    public static Mode of(String name) {
      try {
        return valueOf(name.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("foreach mode [" + name + "] is not supported");
      }
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  @Getter
  @ToString
  @EqualsAndHashCode
  @RequiredArgsConstructor
  public static class ForeachEvalClause {
    private final String targetTemplate;
    private final UnresolvedExpression expression;
  }
}
