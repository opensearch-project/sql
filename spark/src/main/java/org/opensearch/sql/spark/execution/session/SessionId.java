/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;

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

  private static String decode(String sessionId) {
    return new String(Base64.getDecoder().decode(sessionId)).substring(PREFIX_LEN);
  }

  private static String encode(String datasourceName) {
    String randomId = RandomStringUtils.randomAlphanumeric(PREFIX_LEN) + datasourceName;
    return Base64.getEncoder().encodeToString(randomId.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    return "sessionId=" + sessionId;
  }
}
