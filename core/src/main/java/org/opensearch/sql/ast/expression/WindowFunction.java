/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Sort.SortOption;

@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class WindowFunction extends UnresolvedExpression {

  private final UnresolvedExpression function;
  @Setter private List<UnresolvedExpression> partitionByList = new ArrayList<>();
  @Setter private List<Pair<SortOption, UnresolvedExpression>> sortList = new ArrayList<>();
  @Setter private WindowFrame windowFrame = WindowFrame.defaultFrame();

  public WindowFunction(
      UnresolvedExpression function,
      List<UnresolvedExpression> partitionByList,
      List<Pair<SortOption, UnresolvedExpression>> sortList) {
    this.function = function;
    this.partitionByList = partitionByList;
    this.sortList = sortList;
  }

  @Override
  public List<? extends Node> getChild() {
    return List.of(function);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitWindowFunction(this, context);
  }
}
