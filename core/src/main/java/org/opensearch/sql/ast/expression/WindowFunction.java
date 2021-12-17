/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Sort.SortOption;

@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@RequiredArgsConstructor
@ToString
public class WindowFunction extends UnresolvedExpression {

  private final UnresolvedExpression function;
  private List<UnresolvedExpression> partitionByList;
  private List<Pair<SortOption, UnresolvedExpression>> sortList;

  @Override
  public List<? extends Node> getChild() {
    ImmutableList.Builder<UnresolvedExpression> children = ImmutableList.builder();
    children.add(function);
    children.addAll(partitionByList);
    sortList.forEach(pair -> children.add(pair.getRight()));
    return children.build();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitWindowFunction(this, context);
  }

}
