/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.opensearch.sql.spark.execution.session.SessionModel.APPLICATION_ID;
import static org.opensearch.sql.spark.execution.session.SessionModel.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.session.SessionModel.ERROR;
import static org.opensearch.sql.spark.execution.session.SessionModel.JOB_ID;
import static org.opensearch.sql.spark.execution.session.SessionModel.SESSION_DOC_TYPE;
import static org.opensearch.sql.spark.execution.session.SessionModel.SESSION_ID;
import static org.opensearch.sql.spark.execution.session.SessionModel.SESSION_STATE;
import static org.opensearch.sql.spark.execution.session.SessionModel.SESSION_TYPE;
import static org.opensearch.sql.spark.execution.session.SessionModel.VERSION;
import static org.opensearch.sql.spark.execution.statestore.StateModel.LAST_UPDATE_TIME;
import static org.opensearch.sql.spark.execution.statestore.StateModel.TYPE;

import java.io.IOException;
import lombok.SneakyThrows;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.session.SessionType;

public class SessionModelXContentSerializer implements XContentSerializer<SessionModel> {
  @Override
  public XContentBuilder toXContent(SessionModel sessionModel, ToXContent.Params params)
      throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field(VERSION, sessionModel.getVersion())
        .field(TYPE, SESSION_DOC_TYPE)
        .field(SESSION_TYPE, sessionModel.getSessionType().getSessionType())
        .field(SESSION_ID, sessionModel.getSessionId().getSessionId())
        .field(SESSION_STATE, sessionModel.getSessionState().getSessionState())
        .field(DATASOURCE_NAME, sessionModel.getDatasourceName())
        .field(APPLICATION_ID, sessionModel.getApplicationId())
        .field(JOB_ID, sessionModel.getJobId())
        .field(LAST_UPDATE_TIME, sessionModel.getLastUpdateTime())
        .field(ERROR, sessionModel.getError())
        .endObject();
  }

  @Override
  @SneakyThrows
  public SessionModel fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    // Implement the fromXContent logic here
    SessionModel.SessionModelBuilder builder = SessionModel.builder();
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
}
