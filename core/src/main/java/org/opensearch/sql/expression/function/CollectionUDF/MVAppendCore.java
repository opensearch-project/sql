/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;

/** Core logic for `mvappend` command to collect elements from list of args */
public class MVAppendCore {

  /**
   * Collect non-null elements from `args`. If an item is a list, it will collect non-null elements
   * of the list. Each collected element is coerced to {@code elementType} so a heterogeneously
   * boxed input (e.g. an {@code array(int_col)} operand contributing {@code Integer} cells to a
   * {@code BIGINT}-typed result) does not throw {@code ClassCastException} when the array is later
   * materialized by Avatica's per-type accessor. See {@ref MVAppendFunctionImplTest} for detailed
   * behavior.
   */
  /** Untyped overload — collects without element coercion (used by map-append and unit tests). */
  public static List<Object> collectElements(Object... args) {
    return collectElements((SqlTypeName) null, args);
  }

  public static List<Object> collectElements(SqlTypeName elementType, Object... args) {
    List<Object> elements = new ArrayList<>();

    for (Object arg : args) {
      if (arg == null) {
        continue;
      } else if (arg instanceof List) {
        addListElements((List<?>) arg, elements, elementType);
      } else {
        elements.add(coerce(arg, elementType));
      }
    }

    return elements.isEmpty() ? null : elements;
  }

  private static void addListElements(
      List<?> list, List<Object> elements, SqlTypeName elementType) {
    for (Object item : list) {
      if (item != null) {
        elements.add(coerce(item, elementType));
      }
    }
  }

  /**
   * Align a boxed numeric element to the array's target element type. Only numeric widenings that
   * arise from operand widening (e.g. INTEGER cells into a BIGINT array) are handled; non-numeric
   * or null-typed targets pass the value through unchanged so mixed / ANY-typed arrays keep their
   * existing {@code Object[]} runtime semantics.
   */
  private static Object coerce(Object value, SqlTypeName elementType) {
    if (elementType == null || !(value instanceof Number)) {
      return value;
    }
    Number num = (Number) value;
    return switch (elementType) {
      case TINYINT -> num.byteValue();
      case SMALLINT -> num.shortValue();
      case INTEGER -> num.intValue();
      case BIGINT -> num.longValue();
      case FLOAT, REAL -> num.floatValue();
      case DOUBLE -> num.doubleValue();
      case DECIMAL -> num instanceof BigDecimal ? num : BigDecimal.valueOf(num.doubleValue());
      default -> value;
    };
  }
}
