/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class Join extends UnresolvedPlan {
  private UnresolvedPlan left;
  private final UnresolvedPlan right;
  private Optional<String> leftAlias;
  private Optional<String> rightAlias;
  private final JoinType joinType;
  private final Optional<UnresolvedExpression> joinCondition;
  private final JoinHint joinHint;
  private final Optional<List<Field>> joinFields;
  private final Argument.ArgumentMap argumentMap;

  public Join(
      UnresolvedPlan right,
      Optional<String> leftAlias,
      Optional<String> rightAlias,
      JoinType joinType,
      Optional<UnresolvedExpression> joinCondition,
      JoinHint joinHint,
      Optional<List<Field>> joinFields,
      Argument.ArgumentMap argumentMap) {
    this.right = right;
    this.leftAlias = leftAlias;
    this.rightAlias = rightAlias;
    this.joinType = joinType;
    this.joinCondition = joinCondition;
    this.joinHint = joinHint;
    this.joinFields = joinFields;
    this.argumentMap = argumentMap;
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    // attach child to left, meanwhile fill back side aliases if possible.
    if (leftAlias.isPresent()) {
      if (child instanceof SubqueryAlias alias) {
        this.left = new SubqueryAlias(leftAlias.get(), alias.getChild().getFirst());
      } else {
        this.left = new SubqueryAlias(leftAlias.get(), child);
      }
    } else {
      if (child instanceof SubqueryAlias alias) {
        leftAlias = Optional.of(alias.getAlias());
      }
      this.left = child;
    }
    if (rightAlias.isEmpty() && this.right instanceof SubqueryAlias alias) {
      rightAlias = Optional.of(alias.getAlias());
    }
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.left == null ? ImmutableList.of() : ImmutableList.of(this.left);
  }

  public List<UnresolvedPlan> getChildren() {
    return ImmutableList.of(left, right);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitJoin(this, context);
  }

  /**
   * @return `overwrite` option value in argumentMap
   */
  public boolean isOverwrite() {
    return getArgumentMap().get("overwrite") == null // 'overwrite' default value is true
        || getArgumentMap().get("overwrite").equals(Literal.TRUE);
  }

  public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    SEMI,
    ANTI,
    CROSS,
    FULL
  }

  /** RIGHT, CROSS, FULL are performance sensitive join types */
  public static List<JoinType> highCostJoinTypes() {
    return List.of(JoinType.RIGHT, JoinType.CROSS, JoinType.FULL);
  }

  @Getter
  @RequiredArgsConstructor
  public static class JoinHint {
    private final Map<String, String> hints;

    public JoinHint() {
      this.hints = ImmutableMap.of();
    }
  }
}
