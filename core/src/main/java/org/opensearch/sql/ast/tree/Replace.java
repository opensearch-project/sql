/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;

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
