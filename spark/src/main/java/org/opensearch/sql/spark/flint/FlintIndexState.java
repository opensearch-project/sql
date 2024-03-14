/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

/** Flint index state. */
@Getter
public enum FlintIndexState {
  // stable state
  EMPTY("empty"),
  // transitioning state
  CREATING("creating"),
  // transitioning state
  REFRESHING("refreshing"),
  // transitioning state
  CANCELLING("cancelling"),
  // stable state
  ACTIVE("active"),
  // transitioning state
  DELETING("deleting"),
  // stable state
  DELETED("deleted"),
  // stable state
  VACUUMING("vacuuming"),
  // stable state
  FAILED("failed"),
  // unknown state, if some state update in Spark side, not reflect in here.
  UNKNOWN("unknown");

  private final String state;

  FlintIndexState(String state) {
    this.state = state;
  }

  private static Map<String, FlintIndexState> STATES =
      Arrays.stream(FlintIndexState.values())
          .collect(Collectors.toMap(t -> t.name().toLowerCase(), t -> t));

  public static FlintIndexState fromString(String key) {
    for (FlintIndexState ss : FlintIndexState.values()) {
      if (ss.getState().toLowerCase(Locale.ROOT).equals(key)) {
        return ss;
      }
    }
    return UNKNOWN;
  }
}
