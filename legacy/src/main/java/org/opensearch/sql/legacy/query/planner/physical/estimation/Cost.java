/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.estimation;

public class Cost implements Comparable<Cost> {

  public static final Cost INFINITY = new Cost();

  private long inputSize;

  private long time;

  public Cost() {}

  @Override
  public int compareTo(Cost o) {
    return 0;
  }
}
