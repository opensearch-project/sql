/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionState.NOT_STARTED;
import static org.opensearch.sql.spark.execution.session.SessionType.INTERACTIVE;

import java.io.IOException;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Session data in flint.ql.sessions index. */
@Data
@Builder
public class SessionModel extends StateModel {
  public static final String VERSION = "version";
  public static final String TYPE = "type";
  public static final String SESSION_TYPE = "sessionType";
  public static final String SESSION_ID = "sessionId";
  public static final String SESSION_STATE = "state";
  public static final String DATASOURCE_NAME = "dataSourceName";
  public static final String LAST_UPDATE_TIME = "lastUpdateTime";
  public static final String APPLICATION_ID = "applicationId";
  public static final String JOB_ID = "jobId";
  public static final String ERROR = "error";
  public static final String UNKNOWN = "unknown";
  public static final String SESSION_DOC_TYPE = "session";

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

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder
        .startObject()
        .field(VERSION, version)
        .field(TYPE, SESSION_DOC_TYPE)
        .field(SESSION_TYPE, sessionType.getSessionType())
        .field(SESSION_ID, sessionId.getSessionId())
        .field(SESSION_STATE, sessionState.getSessionState())
        .field(DATASOURCE_NAME, datasourceName)
        .field(APPLICATION_ID, applicationId)
        .field(JOB_ID, jobId)
        .field(LAST_UPDATE_TIME, lastUpdateTime)
        .field(ERROR, error)
        .endObject();
    return builder;
  }

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

  @SneakyThrows
  public static SessionModel fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    SessionModelBuilder builder = new SessionModelBuilder();
    XContentParserUtils.ensureExpectedToken(
        XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case VERSION:
          builder.version(parser.text());
          break;
        case SESSION_TYPE:
          builder.sessionType(SessionType.fromString(parser.text()));
          break;
        case SESSION_ID:
          builder.sessionId(new SessionId(parser.text()));
          break;
        case SESSION_STATE:
          builder.sessionState(SessionState.fromString(parser.text()));
          break;
        case DATASOURCE_NAME:
          builder.datasourceName(parser.text());
          break;
        case ERROR:
          builder.error(parser.text());
          break;
        case APPLICATION_ID:
          builder.applicationId(parser.text());
          break;
        case JOB_ID:
          builder.jobId(parser.text());
          break;
        case LAST_UPDATE_TIME:
          builder.lastUpdateTime(parser.longValue());
          break;
        case TYPE:
          // do nothing.
          break;
      }
    }
    builder.seqNo(seqNo);
    builder.primaryTerm(primaryTerm);
    return builder.build();
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
