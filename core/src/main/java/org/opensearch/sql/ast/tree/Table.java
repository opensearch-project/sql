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
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node representing a Table operation in the query plan.
 *
 * <p>This class represents a table operation in the Abstract Syntax Tree (AST). It contains a list
 * of expressions to project (select) from the table and can have a child node representing the
 * source of the data.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class Table extends UnresolvedPlan {
  /**
   * List of expressions to be projected (selected) from the table. These expressions are unresolved
   * at the AST stage.
   */
  private final List<UnresolvedExpression> projectList;

  /**
   * Child node in the query plan tree. This represents the source of data for this table operation.
   */
  private UnresolvedPlan child;

  /**
   * Constructs a Table operation with the specified projection list.
   *
   * @param projectList List of expressions to be projected from the table
   */
  public Table(List<UnresolvedExpression> projectList) {
    this.projectList = projectList;
  }

  /**
   * Attaches a child node to this Table operation.
   *
   * <p>This method implements the parent-child relationship in the query plan tree.
   *
   * @param child The child node to attach to this Table operation
   * @return This Table instance with the child attached
   */
  @Override
  public Table attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  /**
   * Returns the list of child nodes for this Table operation.
   *
   * <p>If no child is attached, returns an empty list. Otherwise, returns a singleton list
   * containing the attached child.
   *
   * @return An immutable list containing the child node, or an empty list if no child exists
   */
  @Override
  public List<UnresolvedPlan> getChild() {
    // Return empty list if no child exists, otherwise return a singleton list with the child
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  /**
   * Accepts a visitor to process this Table node.
   *
   * <p>This method is part of the visitor pattern implementation for traversing the AST. It
   * delegates the processing of this node to the appropriate visitor method.
   *
   * @param <T> The return type of the visitor
   * @param <C> The context type used by the visitor
   * @param nodeVisitor The visitor that will process this node
   * @param context The context to be passed to the visitor
   * @return The result of the visitor's processing
   */
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitTable(this, context);
  }
}
