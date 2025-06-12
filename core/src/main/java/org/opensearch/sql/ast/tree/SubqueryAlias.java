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
import org.opensearch.sql.ast.AbstractNodeVisitor;

@EqualsAndHashCode(callSuper = false)
@ToString
public class SubqueryAlias extends UnresolvedPlan {
  @Getter private final String alias;
  private UnresolvedPlan child;

  /** Create an alias (SubqueryAlias) for a sub-query with a default alias name */
  public SubqueryAlias(UnresolvedPlan child, String suffix) {
    this.alias = "__auto_generated_subquery_name" + suffix;
    this.child = child;
  }

  public SubqueryAlias(String alias, UnresolvedPlan child) {
    this.alias = alias;
    this.child = child;
  }

  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitSubqueryAlias(this, context);
  }
}
