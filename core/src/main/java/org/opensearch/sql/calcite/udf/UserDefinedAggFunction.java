/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf;

public interface UserDefinedAggFunction<S extends UserDefinedAggFunction.Accumulator> {
  /**
   * @return {@link Accumulator}
   */
  S init();

  /**
   *
   * @param {@link Accumulator}
   * @return final result
   */
  Object result(S accumulator);

  /**
   * Add values to the accumulator. Notice some init argument will also be here like the 50 in Percentile(field, 50).
   * @param acc {@link Accumulator}
   * @param values the value to add to accumulator
   * @return {@link Accumulator}
   */
  S add(S acc, Object... values);

  /**
   * Maintain the state when {@link UserDefinedAggFunction} add values
   */
  interface Accumulator {
    /**
      * @return the final aggregation value
     */
    Object value();
  }
}
