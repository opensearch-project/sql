/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Replace extends UnresolvedPlan {
  private final List<ReplacePair> replacePairs;
  private final Set<Field> fieldList;
  @Nullable private UnresolvedPlan child;

  /**
   * Constructor with multiple pattern/replacement pairs.
   *
   * @param replacePairs List of pattern/replacement pairs
   * @param fieldList Set of fields to apply replacements to
   */
  public Replace(List<ReplacePair> replacePairs, Set<Field> fieldList) {
    this.replacePairs = replacePairs;
    this.fieldList = fieldList;
  }

  /**
   * Backward-compatible constructor with single pattern/replacement pair.
   *
   * @param pattern Pattern literal
   * @param replacement Replacement literal
   * @param fieldList Set of fields to apply replacement to
   */
  public Replace(
      UnresolvedExpression pattern, UnresolvedExpression replacement, Set<Field> fieldList) {
    // Convert single pair to list for internal consistency
    if (!(pattern instanceof Literal) || !(replacement instanceof Literal)) {
      throw new IllegalArgumentException(
          "Pattern and replacement must be literals in Replace command");
    }
    this.replacePairs =
        Collections.singletonList(new ReplacePair((Literal) pattern, (Literal) replacement));
    this.fieldList = fieldList;
  }

  /**
   * Get the pattern from the first replacement pair (for backward compatibility).
   *
   * @return Pattern expression
   * @deprecated Use {@link #getReplacePairs()} instead
   */
  @Deprecated
  public UnresolvedExpression getPattern() {
    if (replacePairs.isEmpty()) {
      throw new IllegalStateException("No replacement pairs available");
    }
    return replacePairs.get(0).getPattern();
  }

  /**
   * Get the replacement from the first replacement pair (for backward compatibility).
   *
   * @return Replacement expression
   * @deprecated Use {@link #getReplacePairs()} instead
   */
  @Deprecated
  public UnresolvedExpression getReplacement() {
    if (replacePairs.isEmpty()) {
      throw new IllegalStateException("No replacement pairs available");
    }
    return replacePairs.get(0).getReplacement();
  }

  @Override
  public Replace attach(UnresolvedPlan child) {
    if (null == this.child) {
      this.child = child;
    } else {
      this.child.attach(child);
    }
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitReplace(this, context);
  }
}
