/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;
import static org.opensearch.sql.spark.execution.session.SessionType.INTERACTIVE;

import lombok.Builder;
import lombok.Data;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Session data in flint.ql.sessions index. */
@Data
@Builder
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

  private final long seqNo;
  private final long primaryTerm;

  public static SessionModel of(SessionModel copy, long seqNo, long primaryTerm) {
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
        .seqNo(seqNo)
        .primaryTerm(primaryTerm)
        .build();
  }

  public static SessionModel copyWithState(
      SessionModel copy, SessionState state, long seqNo, long primaryTerm) {
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
        .seqNo(seqNo)
        .primaryTerm(primaryTerm)
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
        .seqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
        .primaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        .build();
  }

  @Override
  public String getId() {
    return sessionId.getSessionId();
  }
}
