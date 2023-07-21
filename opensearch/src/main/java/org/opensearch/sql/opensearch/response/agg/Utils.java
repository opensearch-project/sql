/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response.agg;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Utils {
  /**
   * Utils to handle Nan/Infinite Value.
   * @return null if is Nan or is +-Infinity.
   */
  public static Object handleNanInfValue(double value) {
    return Double.isNaN(value) || Double.isInfinite(value) ? null : value;
  }
}
