/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.opensearch.sql.spark.execution.session.SessionModel.APPLICATION_ID;
import static org.opensearch.sql.spark.execution.session.SessionModel.JOB_ID;
import static org.opensearch.sql.spark.execution.statement.StatementState.WAITING;

import java.io.IOException;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.statestore.StateModel;
import org.opensearch.sql.spark.rest.model.LangType;

/** Statement data in flint.ql.sessions index. */
@Data
@Builder
public class StatementModel extends StateModel {
  public static final String VERSION = "version";
  public static final String TYPE = "type";
  public static final String STATEMENT_STATE = "state";
  public static final String STATEMENT_ID = "statementId";
  public static final String SESSION_ID = "sessionId";
  public static final String LANG = "lang";
  public static final String QUERY = "query";
  public static final String QUERY_ID = "queryId";
  public static final String SUBMIT_TIME = "submitTime";
  public static final String ERROR = "error";
  public static final String UNKNOWN = "unknown";
  public static final String STATEMENT_DOC_TYPE = "statement";

  private final String version;
  private final StatementState statementState;
  private final StatementId statementId;
  private final SessionId sessionId;
  private final String applicationId;
  private final String jobId;
  private final LangType langType;
  private final String query;
  private final String queryId;
  private final long submitTime;
  private final String error;

  private final long seqNo;
  private final long primaryTerm;

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder
        .startObject()
        .field(VERSION, version)
        .field(TYPE, STATEMENT_DOC_TYPE)
        .field(STATEMENT_STATE, statementState.getState())
        .field(STATEMENT_ID, statementId.getId())
        .field(SESSION_ID, sessionId.getSessionId())
        .field(APPLICATION_ID, applicationId)
        .field(JOB_ID, jobId)
        .field(LANG, langType.getText())
        .field(QUERY, query)
        .field(QUERY_ID, queryId)
        .field(SUBMIT_TIME, submitTime)
        .field(ERROR, error)
        .endObject();
    return builder;
  }

  public static StatementModel copy(StatementModel copy, long seqNo, long primaryTerm) {
    return builder()
        .version("1.0")
        .statementState(copy.statementState)
        .statementId(copy.statementId)
        .sessionId(copy.sessionId)
        .applicationId(copy.applicationId)
        .jobId(copy.jobId)
        .langType(copy.langType)
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
        .query(copy.query)
        .queryId(copy.queryId)
        .submitTime(copy.submitTime)
        .error(copy.error)
        .seqNo(seqNo)
        .primaryTerm(primaryTerm)
        .build();
  }

  @SneakyThrows
  public static StatementModel fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    StatementModel.StatementModelBuilder builder = StatementModel.builder();
    XContentParserUtils.ensureExpectedToken(
        XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case VERSION:
          builder.version(parser.text());
          break;
        case TYPE:
          // do nothing
          break;
        case STATEMENT_STATE:
          builder.statementState(StatementState.fromString(parser.text()));
          break;
        case STATEMENT_ID:
          builder.statementId(new StatementId(parser.text()));
          break;
        case SESSION_ID:
          builder.sessionId(new SessionId(parser.text()));
          break;
        case APPLICATION_ID:
          builder.applicationId(parser.text());
          break;
        case JOB_ID:
          builder.jobId(parser.text());
          break;
        case LANG:
          builder.langType(LangType.fromString(parser.text()));
          break;
        case QUERY:
          builder.query(parser.text());
          break;
        case QUERY_ID:
          builder.queryId(parser.text());
          break;
        case SUBMIT_TIME:
          builder.submitTime(parser.longValue());
          break;
        case ERROR:
          builder.error(parser.text());
          break;
      }
    }
    builder.seqNo(seqNo);
    builder.primaryTerm(primaryTerm);
    return builder.build();
  }

  public static StatementModel submitStatement(
      SessionId sid,
      String applicationId,
      String jobId,
      StatementId statementId,
      LangType langType,
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
