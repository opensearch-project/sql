/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.execution.statement.StatementState.WAITING;

import lombok.Builder;
import lombok.Data;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.statestore.StateModel;
import org.opensearch.sql.spark.rest.model.LangType;

/** Statement data in flint.ql.sessions index. */
@Data
@Builder
public class StatementModel extends StateModel {
  public static final String UNKNOWN = "";

  private final String version;
  private final StatementState statementState;
  private final StatementId statementId;
  private final SessionId sessionId;
  private final String applicationId;
  private final String jobId;
  private final LangType langType;
  private final String datasourceName;
  private final String query;
  private final String queryId;
  private final long submitTime;
  private final String error;

  private final long seqNo;
  private final long primaryTerm;

  public static StatementModel copy(StatementModel copy, long seqNo, long primaryTerm) {
    return builder()
        .version("1.0")
        .statementState(copy.statementState)
        .statementId(copy.statementId)
        .sessionId(copy.sessionId)
        .applicationId(copy.applicationId)
        .jobId(copy.jobId)
        .langType(copy.langType)
        .datasourceName(copy.datasourceName)
        .query(copy.query)
        .queryId(copy.queryId)
        .submitTime(copy.submitTime)
        .error(copy.error)
        .seqNo(seqNo)
        .primaryTerm(primaryTerm)
        .build();
  }

  public static StatementModel copyWithState(
      StatementModel copy, StatementState state, long seqNo, long primaryTerm) {
    return builder()
        .version("1.0")
        .statementState(state)
        .statementId(copy.statementId)
        .sessionId(copy.sessionId)
        .applicationId(copy.applicationId)
        .jobId(copy.jobId)
        .langType(copy.langType)
        .datasourceName(copy.datasourceName)
        .query(copy.query)
        .queryId(copy.queryId)
        .submitTime(copy.submitTime)
        .error(copy.error)
        .seqNo(seqNo)
        .primaryTerm(primaryTerm)
        .build();
  }

  public static StatementModel submitStatement(
      SessionId sid,
      String applicationId,
      String jobId,
      StatementId statementId,
      LangType langType,
      String datasourceName,
      String query,
      String queryId) {
    return builder()
        .version("1.0")
        .statementState(WAITING)
        .statementId(statementId)
        .sessionId(sid)
        .applicationId(applicationId)
        .jobId(jobId)
        .langType(langType)
        .datasourceName(datasourceName)
        .query(query)
        .queryId(queryId)
        .submitTime(System.currentTimeMillis())
        .error(UNKNOWN)
        .seqNo(SequenceNumbers.UNASSIGNED_SEQ_NO)
        .primaryTerm(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        .build();
  }

  @Override
  public String getId() {
    return statementId.getId();
  }
}
