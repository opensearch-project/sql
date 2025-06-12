/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/** AST node represent Lookup operation. */
@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Lookup extends UnresolvedPlan {
  private UnresolvedPlan child;
  private final UnresolvedPlan lookupRelation;

  /**
   * Mapping field alias map, inputField -> outputField For example, 1. When an output candidate is
   * "name AS cName", the entry will be name -> cName 2. When an output candidate is "dept", the
   * entry will be dept -> dept
   */
  private final Map<String, String> mappingAliasMap;

  private final OutputStrategy outputStrategy;

  /**
   * Output candidate field map, inputField -> outputField Similar definition as {@link
   * #mappingAliasMap}
   */
  private final Map<String, String> outputAliasMap;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
    return visitor.visitLookup(this, context);
  }

  public enum OutputStrategy {
    APPEND,
    REPLACE
  }
}
