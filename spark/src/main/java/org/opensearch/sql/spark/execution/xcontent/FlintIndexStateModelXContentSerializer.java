/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.APPLICATION_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.ERROR;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.JOB_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.LAST_UPDATE_TIME;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.STATE;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.TYPE;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.VERSION;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.VERSION_1_0;

import java.io.IOException;
import lombok.SneakyThrows;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexStateModel;

public class FlintIndexStateModelXContentSerializer
    implements XContentSerializer<FlintIndexStateModel> {
  public static final String FLINT_INDEX_DOC_TYPE = "flintindexstate";
  public static final String LATEST_ID = "latestId";

  @Override
  public XContentBuilder toXContent(
      FlintIndexStateModel flintIndexStateModel, ToXContent.Params params) throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field(VERSION, VERSION_1_0)
        .field(TYPE, FLINT_INDEX_DOC_TYPE)
        .field(STATE, flintIndexStateModel.getIndexState().getState())
        .field(APPLICATION_ID, flintIndexStateModel.getApplicationId())
        .field(JOB_ID, flintIndexStateModel.getJobId())
        .field(LATEST_ID, flintIndexStateModel.getLatestId())
        .field(DATASOURCE_NAME, flintIndexStateModel.getDatasourceName())
        .field(LAST_UPDATE_TIME, flintIndexStateModel.getLastUpdateTime())
        .field(ERROR, flintIndexStateModel.getError())
        .endObject();
  }

  @Override
  @SneakyThrows
  public FlintIndexStateModel fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    // Implement the fromXContent logic here
    FlintIndexStateModel.FlintIndexStateModelBuilder builder = FlintIndexStateModel.builder();
    XContentParserUtils.ensureExpectedToken(
        XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case STATE:
          builder.indexState(FlintIndexState.fromString(parser.text()));
          break;
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
}
