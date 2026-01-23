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
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    String str = ConversionUtils.preprocessValue(value);
    if (str == null) {
      return null;
    }

    String numberStr = ConversionUtils.extractLeadingNumber(str);
    return numberStr != null ? ConversionUtils.tryParseDouble(numberStr) : null;
  }
}
