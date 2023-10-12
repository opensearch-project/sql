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
public enum SessionType {
  INTERACTIVE("interactive");

  private final String sessionType;

  SessionType(String sessionType) {
    this.sessionType = sessionType;
  }

  private static Map<String, SessionType> TYPES =
      Arrays.stream(SessionType.values())
          .collect(Collectors.toMap(t -> t.name().toLowerCase(), t -> t));

  public static SessionType fromString(String key) {
    if (TYPES.containsKey(key)) {
      return TYPES.get(key);
    }
    throw new IllegalArgumentException("Invalid session type: " + key);
  }
}
