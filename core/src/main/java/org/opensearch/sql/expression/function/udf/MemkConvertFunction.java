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
    return new MemkConvertFunction().convertValue(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    return tryConvertMemoryUnit(preprocessedValue);
  }
}
