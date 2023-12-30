/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

public class RealTimeProvider implements TimeProvider {
  @Override
  public long currentEpochMillis() {
    return System.currentTimeMillis();
  }
}
