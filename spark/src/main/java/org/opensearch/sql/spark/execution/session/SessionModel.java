/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;
import static org.opensearch.sql.spark.execution.session.SessionType.INTERACTIVE;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Session data in flint.ql.sessions index. */
@Data
@SuperBuilder
public class SessionModel extends StateModel {

  public static final String UNKNOWN = "unknown";

  private final String version;
  private final SessionType sessionType;
  private final SessionId sessionId;
  private final SessionState sessionState;
  private final String applicationId;
  private final String jobId;
  private final String datasourceName;
  private final String error;
  private final long lastUpdateTime;

  public static SessionModel of(SessionModel copy, ImmutableMap<String, Object> metadata) {
    return builder()
        .version(copy.version)
        .sessionType(copy.sessionType)
        .sessionId(new SessionId(copy.sessionId.getSessionId()))
        .sessionState(copy.sessionState)
        .datasourceName(copy.datasourceName)
        .applicationId(copy.getApplicationId())
        .jobId(copy.jobId)
        .error(UNKNOWN)
        .lastUpdateTime(copy.getLastUpdateTime())
        .metadata(metadata)
        .build();
  }

  public static SessionModel copyWithState(
      SessionModel copy, SessionState state, ImmutableMap<String, Object> metadata) {
    return builder()
        .version(copy.version)
        .sessionType(copy.sessionType)
        .sessionId(new SessionId(copy.sessionId.getSessionId()))
        .sessionState(state)
        .datasourceName(copy.datasourceName)
        .applicationId(copy.getApplicationId())
        .jobId(copy.jobId)
        .error(UNKNOWN)
        .lastUpdateTime(copy.getLastUpdateTime())
        .metadata(metadata)
        .build();
  }

  public static SessionModel initInteractiveSession(
      String applicationId, String jobId, SessionId sid, String datasourceName) {
    return builder()
        .version("1.0")
        .sessionType(INTERACTIVE)
        .sessionId(sid)
        .sessionState(NOT_STARTED)
        .datasourceName(datasourceName)
        .applicationId(applicationId)
        .jobId(jobId)
        .error(UNKNOWN)
        .lastUpdateTime(System.currentTimeMillis())
        .build();
  }

  @Override
  public String getId() {
    return sessionId.getSessionId();
  }
}
