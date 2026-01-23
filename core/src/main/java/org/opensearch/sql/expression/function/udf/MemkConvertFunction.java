/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

/** PPL memk() conversion function. */
public class MemkConvertFunction extends BaseConversionUDF {

  public MemkConvertFunction() {
    super(MemkConvertFunction.class);
  }

  public static Object convert(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    String str = ConversionUtils.preprocessValue(value);
    if (str == null) {
      return null;
    }

    return ConversionUtils.tryConvertMemoryUnit(str);
  }
}
