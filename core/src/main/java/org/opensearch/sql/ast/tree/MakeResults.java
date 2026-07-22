/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * AST node for the {@code makeresults} leading command (count path). Generates {@code count}
 * in-memory rows, each carrying a single {@code @timestamp} column set to query time.
 *
 * <p>The {@code format=csv|json data="..."} form is parsed into a shared {@link Values} node
 * instead (see {@code MakeResultsDataParser}), so inline literal rows flow through the common
 * {@code visitValues} builder.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class MakeResults extends UnresolvedPlan {

  private final int count;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    throw new UnsupportedOperationException("MakeResults node is supposed to have no child node");
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMakeResults(this, context);
  }

  @Override
  public List<? extends Node> getChild() {
    return ImmutableList.of();
  }
}
