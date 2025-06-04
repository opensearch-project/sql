/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
@RequiredArgsConstructor
@AllArgsConstructor
public class AD extends UnresolvedPlan {
  private UnresolvedPlan child;

  private final Map<String, Literal> arguments;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAD(this, context);
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }
}
