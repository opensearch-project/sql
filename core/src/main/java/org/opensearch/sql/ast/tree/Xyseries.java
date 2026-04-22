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
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node representing the xyseries command. Converts row-oriented grouped results into a wide
 * table where one field is the X axis (row key), one field provides pivot values for column naming,
 * and one or more data fields fill the pivoted cells.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Xyseries extends UnresolvedPlan {

  /** The x-axis field (row key in output). */
  private final UnresolvedExpression xField;

  /** The y-name field whose values become part of the output column names. */
  private final UnresolvedExpression yNameField;

  /** Explicit pivot values from the IN (...) clause. */
  private final List<String> pivotValues;

  /** One or more y-data fields whose values fill the pivoted cells. */
  private final List<UnresolvedExpression> yDataFields;

  /** Separator between y-data-field name and pivot value in column names. Default ":". */
  private final String separator;

  /** Optional format template for output column names using $AGG$ and $VAL$ placeholders. */
  private final String format;

  @Setter private UnresolvedPlan child;

  @Override
  public Xyseries attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitXyseries(this, context);
  }
}
