/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DecimalUtils {

  // same with Spark DecimalType.MAX_PRECISION
  public static int MAX_PRECISION = 38;
  // same with Spark DecimalType.MAX_SCALE
  public static int MAX_SCALE = 38;

  /**
   * Convert double value to BigDecimal. Compare to {@link BigDecimal#valueOf(double)},
   * safeBigDecimal() returns a BigDecimal with its scale is never negative. This method should be
   * only used in analysis and planning.
   *
   * @param value double value
   * @return BigDecimal value
   */
  public static BigDecimal safeBigDecimal(double value) {
    DecimalFormat df = new DecimalFormat();
    df.setMaximumFractionDigits(MAX_SCALE);
    df.setMinimumFractionDigits(1);
    df.setGroupingUsed(false);
    String plain = df.format(value);
    return new BigDecimal(plain);
  }
}
