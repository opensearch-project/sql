/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.exception.SemanticCheckException;

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
    return OpenSearchTypeFactory.isTimeBasedType(fieldType);
  }

  /** Validates that the field type is numeric for numeric binning operations. */
  public static void validateNumericField(RelDataType fieldType, String fieldName) {
    if (!isNumericField(fieldType)) {
      throw new SemanticCheckException(
          String.format(
              "Cannot apply binning: field '%s' is non-numeric and not time-related, expected"
                  + " numeric or time-related type",
              fieldName));
    }
  }

  /** Checks if the field type is numeric. */
  private static boolean isNumericField(RelDataType fieldType) {
    return OpenSearchTypeFactory.isNumericType(fieldType);
  }
}
