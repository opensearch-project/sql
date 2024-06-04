/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.APPLICATION_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.JOB_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.QUERY_ID;
import static org.opensearch.sql.spark.execution.xcontent.XContentCommonAttributes.TYPE;

import java.io.IOException;
import java.util.Locale;
import lombok.SneakyThrows;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.JobType;

public class AsyncQueryJobMetadataXContentSerializer
    implements XContentSerializer<AsyncQueryJobMetadata> {
  public static final String TYPE_JOBMETA = "jobmeta";
  public static final String JOB_TYPE = "jobType";
  public static final String INDEX_NAME = "indexName";
  public static final String RESULT_INDEX = "resultIndex";
  public static final String SESSION_ID = "sessionId";

  @Override
  public XContentBuilder toXContent(AsyncQueryJobMetadata jobMetadata, ToXContent.Params params)
      throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field(QUERY_ID, jobMetadata.getQueryId())
        .field(TYPE, TYPE_JOBMETA)
        .field(JOB_ID, jobMetadata.getJobId())
        .field(APPLICATION_ID, jobMetadata.getApplicationId())
        .field(RESULT_INDEX, jobMetadata.getResultIndex())
        .field(SESSION_ID, jobMetadata.getSessionId())
        .field(DATASOURCE_NAME, jobMetadata.getDatasourceName())
        .field(JOB_TYPE, jobMetadata.getJobType().getText().toLowerCase(Locale.ROOT))
        .field(INDEX_NAME, jobMetadata.getIndexName())
        .endObject();
  }

  @Override
  @SneakyThrows
  public AsyncQueryJobMetadata fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    AsyncQueryJobMetadata.AsyncQueryJobMetadataBuilder builder = AsyncQueryJobMetadata.builder();
    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case QUERY_ID:
          builder.queryId(parser.textOrNull());
          break;
        case JOB_ID:
          builder.jobId(parser.textOrNull());
          break;
        case APPLICATION_ID:
          builder.applicationId(parser.textOrNull());
          break;
        case RESULT_INDEX:
          builder.resultIndex(parser.textOrNull());
          break;
        case SESSION_ID:
          builder.sessionId(parser.textOrNull());
          break;
        case DATASOURCE_NAME:
          builder.datasourceName(parser.textOrNull());
          break;
        case JOB_TYPE:
          String jobTypeStr = parser.textOrNull();
          builder.jobType(
              Strings.isNullOrEmpty(jobTypeStr) ? null : JobType.fromString(jobTypeStr));
          break;
        case INDEX_NAME:
          builder.indexName(parser.textOrNull());
          break;
        case TYPE:
          break;
        default:
          throw new IllegalArgumentException("Unknown field: " + fieldName);
      }
    }
    builder.metadata(XContentSerializerUtil.buildMetadata(seqNo, primaryTerm));
    AsyncQueryJobMetadata result = builder.build();
    if (result.getJobId() == null || result.getApplicationId() == null) {
      throw new IllegalArgumentException("jobId and applicationId are required fields.");
    }
    return builder.build();
  }
}
