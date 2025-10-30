/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import java.util.List;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.CalcitePlanContext;

/** Utility class for bin-specific field operations. */
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
}
