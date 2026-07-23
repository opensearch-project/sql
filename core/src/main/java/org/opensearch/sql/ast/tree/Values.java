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
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.data.type.ExprCoreType;

/** AST node class for a sequence of literal values. */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class Values extends UnresolvedPlan {

  private final List<List<Literal>> values;

  private final List<String> columnNames;

  /**
   * Optional explicit column types, authoritative for the schema. Required to type a zero-row
   * relation (header-only CSV / empty JSON array) where there are no literals to infer from.
   */
  private final List<ExprCoreType> columnTypes;

  /**
   * When {@code true}, prepend an implicit {@code @timestamp = NOW()} column (from {@code
   * makeresults format=json data=}). CSV data= and subsearch callers leave it {@code false}.
   */
  private final boolean withImplicitTimestamp;

  public Values(List<List<Literal>> values) {
    this(values, null, null);
  }

  public Values(
      List<List<Literal>> values, List<String> columnNames, List<ExprCoreType> columnTypes) {
    this(values, columnNames, columnTypes, false);
  }

  public Values(
      List<List<Literal>> values,
      List<String> columnNames,
      List<ExprCoreType> columnTypes,
      boolean withImplicitTimestamp) {
    this.values = values;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.withImplicitTimestamp = withImplicitTimestamp;
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    throw new UnsupportedOperationException("Values node is supposed to have no child node");
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitValues(this, context);
  }

  @Override
  public List<? extends Node> getChild() {
    return ImmutableList.of();
  }
}
