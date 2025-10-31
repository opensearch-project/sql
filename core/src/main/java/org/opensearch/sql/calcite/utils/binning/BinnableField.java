/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Represents a validated field that supports binning operations. The existence of this class
 * guarantees that validation has been run - the field is either numeric or time-based.
 *
 * <p>This design encodes validation in the type system, preventing downstream code from forgetting
 * to validate or running validation multiple times.
 */
@Getter
public class BinnableField {
  private final RexNode fieldExpr;
  private final RelDataType fieldType;
  private final String fieldName;
  private final boolean isTimeBased;
  private final boolean isNumeric;

  /**
   * Creates a validated BinnableField. Throws SemanticCheckException if the field is neither
   * numeric nor time-based.
   *
   * @param fieldExpr The Rex expression for the field
   * @param fieldType The relational data type of the field
   * @param fieldName The name of the field (for error messages)
   * @throws SemanticCheckException if the field is neither numeric nor time-based
   */
  public BinnableField(RexNode fieldExpr, RelDataType fieldType, String fieldName) {
    this.fieldExpr = fieldExpr;
    this.fieldType = fieldType;
    this.fieldName = fieldName;

    this.isTimeBased = OpenSearchTypeFactory.isTimeBasedType(fieldType);
    this.isNumeric = OpenSearchTypeFactory.isNumericType(fieldType);

    // Validation: field must be either numeric or time-based
    if (!isNumeric && !isTimeBased) {
      throw new SemanticCheckException(
          String.format(
              "Cannot apply binning: field '%s' is non-numeric and not time-related, expected"
                  + " numeric or time-related type",
              fieldName));
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
