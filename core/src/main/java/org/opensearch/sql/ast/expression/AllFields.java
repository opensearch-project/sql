/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/** Represent the All fields which is been used in SELECT *. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class AllFields extends UnresolvedExpression {
  /** Whether exclude metadata field by force, only used by calcite engine */
  final boolean excludeMeta;

  public static final AllFields INSTANCE_OF_ALL = new AllFields(false);
  public static final AllFields INSTANCE_EXCEPT_META = new AllFields(true);

  private AllFields(boolean excludeMeta) {
    this.excludeMeta = excludeMeta;
  }

  public static AllFields of() {
    return INSTANCE_OF_ALL;
  }

  public static AllFields excludeMeta() {
    return INSTANCE_EXCEPT_META;
  }

  @Override
  public List<? extends Node> getChild() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitAllFields(this, context);
  }

  public UnresolvedPlan apply(UnresolvedPlan plan) {
    if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
      return plan;
    } else {
      return new Project(ImmutableList.of(this)).attach(plan);
    }
  }
}
