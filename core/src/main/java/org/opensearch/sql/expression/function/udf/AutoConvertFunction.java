/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

/** PPL auto() conversion function. */
public class AutoConvertFunction extends BaseConversionUDF {

  private static final AutoConvertFunction INSTANCE = new AutoConvertFunction();

  public AutoConvertFunction() {
    super(AutoConvertFunction.class);
  }

  public static Object convert(Object value) {
    return INSTANCE.convertValue(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    Double result = tryConvertMemoryUnit(preprocessedValue);
    if (result != null) {
      return result;
    }

    return NumConvertFunction.INSTANCE.applyConversion(preprocessedValue);
  }
}
