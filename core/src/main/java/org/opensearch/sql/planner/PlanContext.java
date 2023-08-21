/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import java.util.Optional;
import lombok.Getter;
import org.opensearch.sql.storage.split.Split;

/** Plan context hold planning related information. */
public class PlanContext {

  @Getter private final Optional<Split> split;

  public PlanContext(Split split) {
    this.split = Optional.of(split);
  }

  private PlanContext(Optional<Split> split) {
    this.split = split;
  }

  public static PlanContext emptyPlanContext() {
    return new PlanContext(Optional.empty());
  }
}
