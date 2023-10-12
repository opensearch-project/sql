/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public enum SessionState {
  NOT_STARTED("not_started"),
  RUNNING("running"),
  DEAD("dead"),
  FAIL("fail");

  private final String sessionState;

  SessionState(String sessionState) {
    this.sessionState = sessionState;
  }

  private static Map<String, SessionState> STATES =
      Arrays.stream(SessionState.values())
          .collect(Collectors.toMap(t -> t.name().toLowerCase(), t -> t));

  public static SessionState fromString(String key) {
    if (STATES.containsKey(key)) {
      return STATES.get(key);
    }
    throw new IllegalArgumentException("Invalid session state: " + key);
  }
}
