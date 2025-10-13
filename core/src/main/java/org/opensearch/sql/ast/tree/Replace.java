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
import org.opensearch.sql.ast.expression.DataType;
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
    validate();
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
    validate();
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

  public void validate() {
    if (replacePairs == null || replacePairs.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one pattern/replacement pair is required in Replace command");
    }

    // Validate each pair
    for (ReplacePair pair : replacePairs) {
      if (pair.getPattern() == null) {
        throw new IllegalArgumentException("Pattern cannot be null in Replace command");
      }
      if (pair.getReplacement() == null) {
        throw new IllegalArgumentException("Replacement cannot be null in Replace command");
      }

      // Validate pattern is a string literal
      if (pair.getPattern().getType() != DataType.STRING) {
        throw new IllegalArgumentException("Pattern must be a string literal in Replace command");
      }

      // Validate replacement is a string literal
      if (pair.getReplacement().getType() != DataType.STRING) {
        throw new IllegalArgumentException(
            "Replacement must be a string literal in Replace command");
      }
    }

    if (fieldList == null || fieldList.isEmpty()) {
      throw new IllegalArgumentException(
          "Field list cannot be empty in Replace command. Use IN clause to specify the field.");
    }
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
