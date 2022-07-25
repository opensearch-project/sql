/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner;

import lombok.Getter;
import lombok.Setter;

/**
 * The context used for planner.
 */
public class PlanContext {
  @Getter
  @Setter
  private int fetchSize;

  public PlanContext() {
    this.fetchSize = 0;
  }
}
