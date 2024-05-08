/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.opensearch.sql.spark.execution.session.SessionModel.APPLICATION_ID;
import static org.opensearch.sql.spark.execution.session.SessionModel.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.session.SessionModel.JOB_ID;
import static org.opensearch.sql.spark.execution.statement.StatementModel.ERROR;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** Flint Index Model maintain the index state. */
@Getter
@Builder
@EqualsAndHashCode(callSuper = false)
public class FlintIndexStateModel extends StateModel {
  public static final String FLINT_INDEX_DOC_TYPE = "flintindexstate";
  public static final String LATEST_ID = "latestId";
  public static final String DOC_ID_PREFIX = "flint";

  private final FlintIndexState indexState;
  private final String applicationId;
  private final String jobId;
  private final String latestId;
  private final String datasourceName;
  private final long lastUpdateTime;
  private final String error;

  @EqualsAndHashCode.Exclude private final long seqNo;
  @EqualsAndHashCode.Exclude private final long primaryTerm;

  public FlintIndexStateModel(
      FlintIndexState indexState,
      String applicationId,
      String jobId,
      String latestId,
      String datasourceName,
      long lastUpdateTime,
      String error,
      long seqNo,
      long primaryTerm) {
    this.indexState = indexState;
    this.applicationId = applicationId;
    this.jobId = jobId;
    this.latestId = latestId;
    this.datasourceName = datasourceName;
    this.lastUpdateTime = lastUpdateTime;
    this.error = error;
    this.seqNo = seqNo;
    this.primaryTerm = primaryTerm;
  }

  public static FlintIndexStateModel copy(FlintIndexStateModel copy, long seqNo, long primaryTerm) {
    return new FlintIndexStateModel(
        copy.indexState,
        copy.applicationId,
        copy.jobId,
        copy.latestId,
        copy.datasourceName,
        copy.lastUpdateTime,
        copy.error,
        seqNo,
        primaryTerm);
  }

  public static FlintIndexStateModel copyWithState(
      FlintIndexStateModel copy, FlintIndexState state, long seqNo, long primaryTerm) {
    return new FlintIndexStateModel(
        state,
        copy.applicationId,
        copy.jobId,
        copy.latestId,
        copy.datasourceName,
        copy.lastUpdateTime,
        copy.error,
        seqNo,
        primaryTerm);
  }

  @SneakyThrows
  public static FlintIndexStateModel fromXContent(
      XContentParser parser, long seqNo, long primaryTerm) {
    FlintIndexStateModelBuilder builder = FlintIndexStateModel.builder();
    XContentParserUtils.ensureExpectedToken(
        XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case STATE:
          builder.indexState(FlintIndexState.fromString(parser.text()));
        case APPLICATION_ID:
          builder.applicationId(parser.text());
          break;
        case JOB_ID:
          builder.jobId(parser.text());
          break;
        case LATEST_ID:
          builder.latestId(parser.text());
          break;
        case DATASOURCE_NAME:
          builder.datasourceName(parser.text());
          break;
        case LAST_UPDATE_TIME:
          builder.lastUpdateTime(parser.longValue());
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

  @Override
  public String getId() {
    return latestId;
  }
}
