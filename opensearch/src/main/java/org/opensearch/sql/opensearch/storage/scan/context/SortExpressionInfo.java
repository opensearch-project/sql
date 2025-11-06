/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Information about a sort expression that has been pushed down to OpenSearch. Contains both the
 * expression and its collation information. For simple field references, stores the field name for
 * stability across schema changes.
 */
@Getter
@AllArgsConstructor
public class SortExpressionInfo {
  /** The RexNode expression being sorted (nullable for simple field references) */
  private final RexNode expression;

  /** The field name for simple field references (nullable for complex expressions) */
  private final String fieldName;

  /** The collation information (direction, null handling) */
  private final RelFieldCollation.Direction direction;

  /** The null direction */
  private final RelFieldCollation.NullDirection nullDirection;

  /**
   * Constructor for complex expressions.
   *
   * @param expression The RexNode expression
   * @param direction Sort direction
   * @param nullDirection Null handling direction
   */
  public SortExpressionInfo(
      RexNode expression,
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection) {
    this(expression, null, direction, nullDirection);
  }

  /**
   * Constructor for simple field references.
   *
   * @param fieldName The field name
   * @param direction Sort direction
   * @param nullDirection Null handling direction
   */
  public SortExpressionInfo(
      String fieldName,
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection) {
    this(null, fieldName, direction, nullDirection);
  }

  /**
   * Check if this is a simple field reference.
   *
   * @return true if this represents a simple field reference, false for complex expressions
   */
  public boolean isSimpleFieldReference() {
    return expression == null && !StringUtils.isEmpty(fieldName);
  }

  /**
   * Get the effective expression for this sort info. For simple field references, creates a
   * RexInputRef based on the current scan schema.
   *
   * @param scan The scan to get the current schema from
   * @return The RexNode expression to use for sorting
   */
  public RexNode getEffectiveExpression(AbstractCalciteIndexScan scan) {
    if (isSimpleFieldReference()) {
      // Find the field index in the current scan schema
      List<String> currentFieldNames = scan.getRowType().getFieldNames();
      int fieldIndex = currentFieldNames.indexOf(fieldName);
      if (fieldIndex >= 0) {
        // Create a RexInputRef for this field
        return scan.getCluster()
            .getRexBuilder()
            .makeInputRef(scan.getRowType().getFieldList().get(fieldIndex).getType(), fieldIndex);
      }
      // Field not found in current schema - this shouldn't happen in normal cases
      return null;
    } else {
      // Complex expression - return as-is
      return expression;
    }
  }

  /**
   * Create a RelFieldCollation for this sort expression at the given field index.
   *
   * @param fieldIndex The field index in the output schema
   * @return RelFieldCollation representing this sort expression
   */
  public RelFieldCollation toRelFieldCollation(int fieldIndex) {
    return new RelFieldCollation(fieldIndex, direction, nullDirection);
  }

  @Override
  public String toString() {
    String sortTarget = isSimpleFieldReference() ? fieldName : expression.toString();
    return String.format("%s %s %s", sortTarget, direction.toString(), nullDirection.toString());
  }
}
