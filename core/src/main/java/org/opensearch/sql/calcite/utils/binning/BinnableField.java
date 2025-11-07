/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * Represents a field that supports binning operations. Supports numeric, time-based, and string
 * fields. Type coercion for string fields is handled automatically by Calcite's type system.
 */
@Getter
public class BinnableField {
  private final RexNode fieldExpr;
  private final RelDataType fieldType;
  private final String fieldName;
  private final boolean isTimeBased;
  private final boolean isNumeric;

  /**
   * Creates a BinnableField for binning operations. Supports numeric, time-based, and string
   * fields. Type coercion for string fields is handled by Calcite's type system.
   *
   * @param fieldExpr The Rex expression for the field
   * @param fieldType The relational data type of the field
   * @param fieldName The name of the field (for error messages)
   */
  public BinnableField(RexNode fieldExpr, RelDataType fieldType, String fieldName) {
    this.fieldExpr = fieldExpr;
    this.fieldType = fieldType;
    this.fieldName = fieldName;

    this.isTimeBased = OpenSearchTypeFactory.isTimeBasedType(fieldType);
    this.isNumeric = OpenSearchTypeFactory.isNumericType(fieldType);
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
