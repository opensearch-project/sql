/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.aggregation;

/** Shared checked addition for the native BIGINT sum aggregation. */
final class CheckedLongSum {

  static final String OVERFLOW_MESSAGE = "BIGINT overflow in SUM";

  private CheckedLongSum() {}

  static long add(long left, long right) {
    try {
      return Math.addExact(left, right);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(OVERFLOW_MESSAGE, e);
    }
  }
}
