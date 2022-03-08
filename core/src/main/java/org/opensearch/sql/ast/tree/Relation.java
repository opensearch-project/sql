/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Logical plan node of Relation, the interface for building the searching sources.
 */
@AllArgsConstructor
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Relation extends UnresolvedPlan {
  private static final String COMMA = ",";

  private final List<UnresolvedExpression> tableName;

  public Relation(UnresolvedExpression tableName) {
    this(tableName, null);
  }

  public Relation(UnresolvedExpression tableName, String alias) {
    this.tableName = Arrays.asList(tableName);
    this.alias = alias;
  }

  /**
   * Optional alias name for the relation.
   */
  private String alias;

  /**
   * Get original table name. Unwrap and get name if table name expression
   * is actually an Alias.
   * @return    table name
   */
  public String getTableName() {
    return tableName.stream()
        .map(UnresolvedExpression::toString)
        .collect(Collectors.joining(COMMA));
  }

  /**
   * Get original table name or its alias if present in Alias.
   * @return    table name or its alias
   */
  public String getTableNameOrAlias() {
    return (alias == null) ? getTableName() : alias;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRelation(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return this;
  }
}
