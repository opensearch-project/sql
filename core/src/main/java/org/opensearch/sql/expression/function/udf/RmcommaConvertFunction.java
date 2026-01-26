/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

/** PPL rmcomma() conversion function. */
public class RmcommaConvertFunction extends BaseConversionUDF {

  public RmcommaConvertFunction() {
    super(RmcommaConvertFunction.class);
  }

  public static Object convert(Object value) {
    return new RmcommaConvertFunction().convertValue(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    if (containsLetter(preprocessedValue)) {
      return null;
    }
    return tryConvertWithCommaRemoval(preprocessedValue);
  }
}
