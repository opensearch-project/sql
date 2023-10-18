/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;

@Data
public class SessionId {
  private final String sessionId;

  public static SessionId newSessionId() {
    return new SessionId(RandomStringUtils.randomAlphanumeric(16));
  }

  @Override
  public String toString() {
    return "sessionId=" + sessionId;
  }
}
