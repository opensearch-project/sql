/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.time.Clock;
import lombok.Getter;

/**
 * Class to capture values that may be necessary to implement some functions.
 * An example would be query execution start time to implement now().
 */
public class FunctionProperties {

  public FunctionProperties(Clock systemClock) {
    this.systemClock = systemClock;
    this.queryStartClock = Clock.fixed(systemClock.instant(), systemClock.getZone());
  }

  /**
   * Method to get time when query began execution.
   * Clock class combines an instant Supplier and a time zone.
   * @return a fixed clock that returns the time execution started at.
   *
   */
  @Getter
  private final Clock queryStartClock;

  /**
   * Method to access current system clock.
   * @return a ticking clock that tells the time.
   */
  @Getter
  private final Clock systemClock;
}

