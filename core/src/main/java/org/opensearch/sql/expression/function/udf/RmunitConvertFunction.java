/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

/** PPL rmunit() conversion function. */
public class RmunitConvertFunction extends BaseConversionUDF {

  public RmunitConvertFunction() {
    super(RmunitConvertFunction.class);
  }

  public static Object convert(Object value) {
    return new RmunitConvertFunction().convertValue(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    String numberStr = extractLeadingNumber(preprocessedValue);
    return numberStr != null ? tryParseDouble(numberStr) : null;
  }
}
