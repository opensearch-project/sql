/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

/** PPL num() conversion function. */
public class NumConvertFunction extends BaseConversionUDF {

  public NumConvertFunction() {
    super("numConvert", ConversionStrategy.STANDARD);
  }
}
