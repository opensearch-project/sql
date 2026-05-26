/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;

@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class MvCombine extends UnresolvedPlan {

  private final Field field;
  private final String delim;
  @Nullable private UnresolvedPlan child;

  public MvCombine(Field field, @Nullable String delim) {
    this.field = field;
    this.delim = (delim == null) ? " " : delim;
  }

  public MvCombine attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return child == null ? ImmutableList.of() : ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMvCombine(this, context);
  }
}
