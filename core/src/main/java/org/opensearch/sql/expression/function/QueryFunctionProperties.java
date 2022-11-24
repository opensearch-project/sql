/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * Class to capture values that may be necessary to implement some functions.
 * An example would be query execution start time to implement now().
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class QueryFunctionProperties implements FunctionProperties {

  private final Instant nowInstant;
  private final ZoneId currentZoneId;

  /**
   * By default, use current time and current timezone.
   */
  public QueryFunctionProperties() {
    nowInstant = Instant.now();
    currentZoneId = ZoneId.systemDefault();
  }

  /**
   * Method to access current system clock.
   * @return a ticking clock that tells the time.
   */
  @Override
  public Clock getSystemClock() {
    return Clock.system(currentZoneId);
  }

  /**
   * Method to get time when query began execution.
   * Clock class combines an instant Supplier and a time zone.
   * @return a fixed clock that returns the time execution started at.
   *
   */
  @Override
  public Clock getQueryStartClock() {
    return Clock.fixed(nowInstant, currentZoneId);
  }
}

