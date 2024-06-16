/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

public class MockTimeProvider implements TimeProvider {
  private final long fixedTime;

  public MockTimeProvider(long fixedTime) {
    this.fixedTime = fixedTime;
  }

  @Override
  public long currentEpochMillis() {
    return fixedTime;
  }
}
