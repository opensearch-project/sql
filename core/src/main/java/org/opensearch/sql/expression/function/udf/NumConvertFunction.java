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
    return new NumConvertFunction().convertValue(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    if (!isPotentiallyConvertible(preprocessedValue)) {
      return null;
    }

    Double result = tryParseDouble(preprocessedValue);
    if (result != null) {
      return result;
    }

    if (preprocessedValue.contains(",")) {
      result = tryConvertWithCommaRemoval(preprocessedValue);
      if (result != null) {
        return result;
      }
    }

    String leadingNumber = extractLeadingNumber(preprocessedValue);
    if (hasValidUnitSuffix(preprocessedValue, leadingNumber)) {
      return tryParseDouble(leadingNumber);
    }

    return null;
  }
}
