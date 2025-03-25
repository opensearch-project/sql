/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.executor.QueryType;

@EqualsAndHashCode
public class FunctionProperties implements Serializable {

  private final Instant nowInstant;
  private final ZoneId currentZoneId;
  @Getter private final QueryType queryType;

  /** By default, use current time and current timezone. */
  public FunctionProperties() {
    this(QueryType.SQL);
  }

  public FunctionProperties(QueryType queryType) {
    this(Instant.now(), ZoneId.systemDefault(), queryType);
  }

  public FunctionProperties(Instant nowInstant, ZoneId currentZoneId) {
    this(nowInstant, currentZoneId, QueryType.SQL);
  }

  public FunctionProperties(Instant nowInstant, ZoneId currentZoneId, QueryType queryType) {
    this.nowInstant = nowInstant;
    this.currentZoneId = currentZoneId;
    this.queryType = queryType;
  }

  /**
   * Method to access current system clock.
   *
   * @return a ticking clock that tells the time.
   */
  public Clock getSystemClock() {
    return Clock.system(currentZoneId);
  }

  /**
   * Method to get time when query began execution. Clock class combines an instant Supplier and a
   * time zone.
   *
   * @return a fixed clock that returns the time execution started at.
   */
  public Clock getQueryStartClock() {
    return Clock.fixed(nowInstant, currentZoneId);
  }

  /** Use when compiling functions that do not rely on function properties. */
  public static final FunctionProperties None =
      new FunctionProperties() {
        @Override
        public Clock getSystemClock() {
          throw new UnexpectedCallException();
        }

        @Override
        public Clock getQueryStartClock() {
          throw new UnexpectedCallException();
        }
      };

  class UnexpectedCallException extends RuntimeException {
    public UnexpectedCallException() {
      super("FunctionProperties.None is a null object and not meant to be accessed.");
    }
  }
}
