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

/** AST node representing the {@code mvexpand} PPL command: {@code mvexpand <field> [limit=N]}. */
@ToString
@EqualsAndHashCode(callSuper = false)
public class MvExpand extends UnresolvedPlan {

  private UnresolvedPlan child;
  @Getter private final Field field;
  @Getter @Nullable private final Integer limit;

  public MvExpand(Field field, @Nullable Integer limit) {
    this.field = field;
    this.limit = limit;
  }

  @Override
  public MvExpand attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMvExpand(this, context);
  }
}
