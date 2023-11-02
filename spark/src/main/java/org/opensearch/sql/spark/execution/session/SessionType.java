/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.Locale;
import lombok.Getter;

@Getter
public enum SessionType {
  INTERACTIVE("interactive"),
  STREAMING("streaming"),
  BATCH("batch");

  private final String sessionType;

  SessionType(String sessionType) {
    this.sessionType = sessionType;
  }

  public static SessionType fromString(String key) {
    for (SessionType sType : SessionType.values()) {
      if (sType.getSessionType().toLowerCase(Locale.ROOT).equals(key)) {
        return sType;
      }
    }
    throw new IllegalArgumentException("Invalid session type: " + key);
  }
}
