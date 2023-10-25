/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.utils.IDUtils.decode;
import static org.opensearch.sql.spark.utils.IDUtils.encode;

import lombok.Data;

@Data
public class SessionId {
  public static final int PREFIX_LEN = 10;

  private final String sessionId;

  public static SessionId newSessionId(String datasourceName) {
    return new SessionId(encode(datasourceName));
  }

  public String getDataSourceName() {
    return decode(sessionId);
  }

  @Override
  public String toString() {
    return "sessionId=" + sessionId;
  }
}
