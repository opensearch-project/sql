/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

/** PPL num() conversion function. */
public class NumConvertFunction extends BaseConversionUDF {

  public NumConvertFunction() {
    super(NumConvertFunction.class);
  }

  public static Object convert(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    String str = ConversionUtils.preprocessValue(value);
    if (str == null || !ConversionUtils.isPotentiallyConvertible(str)) {
      return null;
    }

    Double result = ConversionUtils.tryParseDouble(str);
    if (result != null) {
      return result;
    }

    if (str.contains(",")) {
      result = ConversionUtils.tryConvertWithCommaRemoval(str);
      if (result != null) {
        return result;
      }
    }

    String leadingNumber = ConversionUtils.extractLeadingNumber(str);
    if (ConversionUtils.hasValidUnitSuffix(str, leadingNumber)) {
      return ConversionUtils.tryParseDouble(leadingNumber);
    }

    return null;
  }
}
