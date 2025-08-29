/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Utility class for field validation and type checking in bin operations. */
public class BinFieldValidator {

  /** Extracts the field name from a Bin node. */
  public static String extractFieldName(Bin node) {
    if (node.getField() instanceof Field) {
      Field field = (Field) node.getField();
      return field.getField().toString();
    } else {
      return node.getField().toString();
    }
  }

  /** Validates that the specified field exists in the dataset. */
  public static void validateFieldExists(String fieldName, CalcitePlanContext context) {
    List<String> availableFields = context.relBuilder.peek().getRowType().getFieldNames();
    if (!availableFields.contains(fieldName)) {
      throw new IllegalArgumentException(
          String.format(
              "Field '%s' not found in dataset. Available fields: %s", fieldName, availableFields));
    }
  }

  /** Checks if the field type is time-based. */
  public static boolean isTimeBasedField(RelDataType fieldType) {
    // Check standard SQL time types
    SqlTypeName sqlType = fieldType.getSqlTypeName();
    if (sqlType == SqlTypeName.TIMESTAMP
        || sqlType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || sqlType == SqlTypeName.DATE) {
      return true;
    }

    // Check for OpenSearch UDT types (EXPR_TIMESTAMP mapped to VARCHAR)
    if (fieldType instanceof AbstractExprRelDataType<?>) {
      AbstractExprRelDataType<?> exprType = (AbstractExprRelDataType<?>) fieldType;
      ExprType udtType = exprType.getExprType();
      return udtType == ExprCoreType.TIMESTAMP
          || udtType == ExprCoreType.DATE
          || udtType == ExprCoreType.TIME;
    }

    // Check if type string contains EXPR_TIMESTAMP
    return fieldType.toString().contains("EXPR_TIMESTAMP");
  }
}
