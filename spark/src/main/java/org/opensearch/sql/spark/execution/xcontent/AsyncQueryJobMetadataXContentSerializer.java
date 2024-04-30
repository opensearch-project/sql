/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata.INDEX_NAME;
import static org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata.JOB_TYPE;
import static org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata.TYPE_JOBMETA;
import static org.opensearch.sql.spark.execution.session.SessionModel.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.statement.StatementModel.QUERY_ID;

import java.io.IOException;
import java.util.Locale;
import lombok.SneakyThrows;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.model.JobType;

public class AsyncQueryJobMetadataXContentSerializer
    implements XContentSerializer<AsyncQueryJobMetadata> {
  @Override
  public XContentBuilder toXContent(AsyncQueryJobMetadata jobMetadata, ToXContent.Params params)
      throws IOException {
    return XContentFactory.jsonBuilder()
        .startObject()
        .field(QUERY_ID, jobMetadata.getQueryId().getId())
        .field("type", TYPE_JOBMETA)
        .field("jobId", jobMetadata.getJobId())
        .field("applicationId", jobMetadata.getApplicationId())
        .field("resultIndex", jobMetadata.getResultIndex())
        .field("sessionId", jobMetadata.getSessionId())
        .field(DATASOURCE_NAME, jobMetadata.getDatasourceName())
        .field(JOB_TYPE, jobMetadata.getJobType().getText().toLowerCase(Locale.ROOT))
        .field(INDEX_NAME, jobMetadata.getIndexName())
        .endObject();
  }

  @Override
  @SneakyThrows
  public AsyncQueryJobMetadata fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    AsyncQueryId queryId = null;
    String jobId = null;
    String applicationId = null;
    String resultIndex = null;
    String sessionId = null;
    String datasourceName = null;
    String jobTypeStr = null;
    String indexName = null;
    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case QUERY_ID:
          queryId = new AsyncQueryId(parser.textOrNull());
          break;
        case "jobId":
          jobId = parser.textOrNull();
          break;
        case "applicationId":
          applicationId = parser.textOrNull();
          break;
        case "resultIndex":
          resultIndex = parser.textOrNull();
          break;
        case "sessionId":
          sessionId = parser.textOrNull();
          break;
        case DATASOURCE_NAME:
          datasourceName = parser.textOrNull();
        case JOB_TYPE:
          jobTypeStr = parser.textOrNull();
        case INDEX_NAME:
          indexName = parser.textOrNull();
        case "type":
          break;
        default:
          throw new IllegalArgumentException("Unknown field: " + fieldName);
      }
    }
    if (jobId == null || applicationId == null) {
      throw new IllegalArgumentException("jobId and applicationId are required fields.");
    }
    return new AsyncQueryJobMetadata(
        queryId,
        applicationId,
        jobId,
        resultIndex,
        sessionId,
        datasourceName,
        Strings.isNullOrEmpty(jobTypeStr) ? null : JobType.fromString(jobTypeStr),
        indexName,
        seqNo,
        primaryTerm);
  }
}
