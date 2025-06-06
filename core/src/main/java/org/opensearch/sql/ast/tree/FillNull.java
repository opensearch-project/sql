/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node represent FillNull operation. */
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
public class FillNull extends UnresolvedPlan {

  public static FillNull ofVariousValue(List<Pair<Field, UnresolvedExpression>> replacements) {
    return new FillNull(replacements);
  }

  public static FillNull ofSameValue(UnresolvedExpression replacement, List<Field> fieldList) {
    List<Pair<Field, UnresolvedExpression>> replacementPairs =
        fieldList.stream().map(f -> Pair.of(f, replacement)).collect(Collectors.toList());
    FillNull instance = new FillNull(replacementPairs);
    if (replacementPairs.isEmpty()) {
      // no field specified, the replacement value will be applied to all fields.
      instance.replacementForAll = Optional.of(replacement);
    }
    return instance;
  }

  private Optional<UnresolvedExpression> replacementForAll = Optional.empty();

  private final List<Pair<Field, UnresolvedExpression>> replacementPairs;

  FillNull(List<Pair<Field, UnresolvedExpression>> replacementPairs) {
    this.replacementPairs = replacementPairs;
  }

  private UnresolvedPlan child;

  public List<Field> getFields() {
    return getReplacementPairs().stream().map(Pair::getLeft).collect(Collectors.toList());
  }

  @Override
  public FillNull attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return child == null ? List.of() : List.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitFillNull(this, context);
  }
}
