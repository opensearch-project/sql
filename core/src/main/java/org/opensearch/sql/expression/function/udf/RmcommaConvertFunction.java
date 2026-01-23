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
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    String str = ConversionUtils.preprocessValue(value);
    if (str == null) {
      return null;
    }

    if (ConversionUtils.CONTAINS_LETTER_PATTERN.matcher(str).matches()) {
      return null;
    }

    return ConversionUtils.tryConvertWithCommaRemoval(str);
  }
}
