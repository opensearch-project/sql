/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

/** {@link Statement} State. */
@Getter
public enum StatementState {
  WAITING("waiting"),
  RUNNING("running"),
  SUCCESS("success"),
  FAILED("failed"),
  CANCELLED("cancelled");

  private final String state;

  StatementState(String state) {
    this.state = state;
  }

  private static Map<String, StatementState> STATES =
      Arrays.stream(StatementState.values())
          .collect(Collectors.toMap(t -> t.name().toLowerCase(), t -> t));

  public static Set<StatementState> NOT_CANCELLABLE_STATE =
      ImmutableSet.of(SUCCESS, FAILED, CANCELLED);

  public static StatementState fromString(String key) {
    for (StatementState ss : StatementState.values()) {
      if (ss.getState().toLowerCase(Locale.ROOT).equals(key)) {
        return ss;
      }
    }
    throw new IllegalArgumentException("Invalid statement state: " + key);
  }
}
