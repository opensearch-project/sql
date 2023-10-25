/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.sql.spark.execution.statement.StatementModel.QUERY_ID;

import com.google.gson.Gson;
import java.io.IOException;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** This class models all the metadata required for a job. */
@Data
@EqualsAndHashCode(callSuper = false)
public class AsyncQueryJobMetadata extends StateModel {
  public static final String TYPE_JOBMETA = "jobmeta";

  private final AsyncQueryId queryId;
  private final String applicationId;
  private final String jobId;
  private final boolean isDropIndexQuery;
  private final String resultIndex;
  // optional sessionId.
  private final String sessionId;

  @EqualsAndHashCode.Exclude private final long seqNo;
  @EqualsAndHashCode.Exclude private final long primaryTerm;

  public AsyncQueryJobMetadata(
      AsyncQueryId queryId, String applicationId, String jobId, String resultIndex) {
    this(
        queryId,
        applicationId,
        jobId,
        false,
        resultIndex,
        null,
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }

  public AsyncQueryJobMetadata(
      AsyncQueryId queryId,
      String applicationId,
      String jobId,
      boolean isDropIndexQuery,
      String resultIndex,
      String sessionId) {
    this(
        queryId,
        applicationId,
        jobId,
        isDropIndexQuery,
        resultIndex,
        sessionId,
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }

  public AsyncQueryJobMetadata(
      AsyncQueryId queryId,
      String applicationId,
      String jobId,
      boolean isDropIndexQuery,
      String resultIndex,
      String sessionId,
      long seqNo,
      long primaryTerm) {
    this.queryId = queryId;
    this.applicationId = applicationId;
    this.jobId = jobId;
    this.isDropIndexQuery = isDropIndexQuery;
    this.resultIndex = resultIndex;
    this.sessionId = sessionId;
    this.seqNo = seqNo;
    this.primaryTerm = primaryTerm;
  }

  @Override
  public String toString() {
    return new Gson().toJson(this);
  }

  /**
   * Converts JobMetadata to XContentBuilder.
   *
   * @return XContentBuilder {@link XContentBuilder}
   * @throws Exception Exception.
   */
  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder
        .startObject()
        .field(QUERY_ID, queryId.getId())
        .field("type", TYPE_JOBMETA)
        .field("jobId", jobId)
        .field("applicationId", applicationId)
        .field("isDropIndexQuery", isDropIndexQuery)
        .field("resultIndex", resultIndex)
        .field("sessionId", sessionId)
        .endObject();
    return builder;
  }

  /** copy builder. update seqNo and primaryTerm */
  public static AsyncQueryJobMetadata copy(
      AsyncQueryJobMetadata copy, long seqNo, long primaryTerm) {
    return new AsyncQueryJobMetadata(
        copy.getQueryId(),
        copy.getApplicationId(),
        copy.getJobId(),
        copy.isDropIndexQuery(),
        copy.getResultIndex(),
        copy.getSessionId(),
        seqNo,
        primaryTerm);
  }

  /**
   * Convert xcontent parser to JobMetadata.
   *
   * @param parser parser.
   * @return JobMetadata {@link AsyncQueryJobMetadata}
   * @throws IOException IOException.
   */
  @SneakyThrows
  public static AsyncQueryJobMetadata fromXContent(
      XContentParser parser, long seqNo, long primaryTerm) {
    AsyncQueryId queryId = null;
    String jobId = null;
    String applicationId = null;
    boolean isDropIndexQuery = false;
    String resultIndex = null;
    String sessionId = null;
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
        case "isDropIndexQuery":
          isDropIndexQuery = parser.booleanValue();
          break;
        case "resultIndex":
          resultIndex = parser.textOrNull();
          break;
        case "sessionId":
          sessionId = parser.textOrNull();
          break;
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
        isDropIndexQuery,
        resultIndex,
        sessionId,
        seqNo,
        primaryTerm);
  }

  @Override
  public String getId() {
    return queryId.docId();
  }
}
