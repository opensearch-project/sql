/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.exception.SemanticCheckException;

/** Represents a field that supports binning operations. */
@Getter
public class BinnableField {
  private final RexNode fieldExpr;
  private final RelDataType fieldType;
  private final String fieldName;
  private final boolean isTimeBased;
  private final boolean isNumeric;

  /**
   * Creates a BinnableField. Validates that the field type is compatible with binning operations.
   *
   * @param fieldExpr The Rex expression for the field
   * @param fieldType The relational data type of the field
   * @param fieldName The name of the field (for error messages)
   * @throws SemanticCheckException if the field type is not supported for binning
   */
  public BinnableField(RexNode fieldExpr, RelDataType fieldType, String fieldName) {
    this.fieldExpr = fieldExpr;
    this.fieldType = fieldType;
    this.fieldName = fieldName;

    this.isTimeBased = OpenSearchTypeUtil.isDatetime(fieldType);
    this.isNumeric = OpenSearchTypeUtil.isNumericOrCharacter(fieldType);

    // Reject truly unsupported types (e.g., BOOLEAN, ARRAY, MAP)
    if (!isNumeric && !isTimeBased) {
      throw new SemanticCheckException(
          String.format("Cannot apply binning to field '%s': unsupported type", fieldName));
    }
  }

  /**
   * Returns true if this field requires numeric binning logic (not time-based binning).
   *
   * @return true if the field should use numeric binning, false if it should use time-based binning
   */
  public boolean requiresNumericBinning() {
    return !isTimeBased;
  }
}
