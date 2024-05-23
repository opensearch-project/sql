/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer.SESSION_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.APPLICATION_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.ERROR;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.JOB_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.STATE;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.TYPE;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.VERSION;

import java.io.IOException;
import lombok.SneakyThrows;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.rest.model.LangType;

public class StatementModelXContentSerializer implements XContentSerializer<StatementModel> {
  public static final String STATEMENT_DOC_TYPE = "statement";
  public static final String STATEMENT_ID = "statementId";
  public static final String LANG = "lang";
  public static final String QUERY = "query";
  public static final String QUERY_ID = "queryId";
  public static final String SUBMIT_TIME = "submitTime";
  public static final String UNKNOWN = "";

  @Override
  public XContentBuilder toXContent(StatementModel statementModel, ToXContent.Params params)
      throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field(VERSION, statementModel.getVersion())
        .field(TYPE, STATEMENT_DOC_TYPE)
        .field(STATE, statementModel.getStatementState().getState())
        .field(STATEMENT_ID, statementModel.getStatementId().getId())
        .field(SESSION_ID, statementModel.getSessionId().getSessionId())
        .field(APPLICATION_ID, statementModel.getApplicationId())
        .field(JOB_ID, statementModel.getJobId())
        .field(LANG, statementModel.getLangType().getText())
        .field(DATASOURCE_NAME, statementModel.getDatasourceName())
        .field(QUERY, statementModel.getQuery())
        .field(QUERY_ID, statementModel.getQueryId())
        .field(SUBMIT_TIME, statementModel.getSubmitTime())
        .field(ERROR, statementModel.getError())
        .endObject();
  }

  @Override
  @SneakyThrows
  public StatementModel fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
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
        case STATE:
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
        case DATASOURCE_NAME:
          builder.datasourceName(parser.text());
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
        default:
          throw new IllegalArgumentException("Unexpected field: " + fieldName);
      }
    }
    builder.metadata(XContentSerializerUtil.buildMetadata(seqNo, primaryTerm));
    return builder.build();
  }
}
