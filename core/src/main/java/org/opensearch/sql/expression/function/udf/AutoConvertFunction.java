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
    return new AutoConvertFunction().convertValue(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    Double result = tryConvertMemoryUnit(preprocessedValue);
    if (result != null) {
      return result;
    }

    return new NumConvertFunction().applyConversion(preprocessedValue);
  }
}
