/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

/** PPL auto() conversion function. */
public class AutoConvertFunction extends BaseConversionUDF {

  public AutoConvertFunction() {
    super(AutoConvertFunction.class);
  }

  public static Object convert(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    String str = ConversionUtils.preprocessValue(value);
    if (str == null) {
      return null;
    }

    Double result = ConversionUtils.tryConvertMemoryUnit(str);
    if (result != null) {
      return result;
    }

    return NumConvertFunction.convert(value);
  }
}
