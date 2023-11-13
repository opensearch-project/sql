/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import java.util.Arrays;
import java.util.Map;
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

  public static StatementState fromString(String key) {
    if (STATES.containsKey(key)) {
      return STATES.get(key);
    }
    throw new IllegalArgumentException("Invalid statement state: " + key);
  }
}
