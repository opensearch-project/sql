/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.sql.spark.execution.session.SessionModel.DATASOURCE_NAME;
import static org.opensearch.sql.spark.execution.statement.StatementModel.QUERY_ID;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Locale;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** This class models all the metadata required for a job. */
@Data
@EqualsAndHashCode(callSuper = false)
public class AsyncQueryJobMetadata extends StateModel {
  public static final String TYPE_JOBMETA = "jobmeta";
  public static final String JOB_TYPE = "jobType";
  public static final String INDEX_NAME = "indexName";

  private final AsyncQueryId queryId;
  private final String applicationId;
  private final String jobId;
  private final String resultIndex;
  // optional sessionId.
  private final String sessionId;
  // since 2.13
  // jobType could be null before OpenSearch 2.12. SparkQueryDispatcher use jobType to choose
  // cancel query handler. if jobType is null, it will invoke BatchQueryHandler.cancel().
  private final JobType jobType;
  // null if JobType is null
  private final String datasourceName;
  // null if JobType is INTERACTIVE or null
  private final String indexName;

  @EqualsAndHashCode.Exclude private final long seqNo;
  @EqualsAndHashCode.Exclude private final long primaryTerm;

  public AsyncQueryJobMetadata(
      AsyncQueryId queryId, String applicationId, String jobId, String resultIndex) {
    this(
        queryId,
        applicationId,
        jobId,
        resultIndex,
        null,
        null,
        JobType.INTERACTIVE,
        null,
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }

  public AsyncQueryJobMetadata(
      AsyncQueryId queryId,
      String applicationId,
      String jobId,
      String resultIndex,
      String sessionId) {
    this(
        queryId,
        applicationId,
        jobId,
        resultIndex,
        sessionId,
        null,
        JobType.INTERACTIVE,
        null,
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }

  public AsyncQueryJobMetadata(
      AsyncQueryId queryId,
      String applicationId,
      String jobId,
      String resultIndex,
      String sessionId,
      String datasourceName,
      JobType jobType,
      String indexName) {
    this(
        queryId,
        applicationId,
        jobId,
        resultIndex,
        sessionId,
        datasourceName,
        jobType,
        indexName,
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }

  public AsyncQueryJobMetadata(
      AsyncQueryId queryId,
      String applicationId,
      String jobId,
      String resultIndex,
      String sessionId,
      String datasourceName,
      JobType jobType,
      String indexName,
      long seqNo,
      long primaryTerm) {
    this.queryId = queryId;
    this.applicationId = applicationId;
    this.jobId = jobId;
    this.resultIndex = resultIndex;
    this.sessionId = sessionId;
    this.datasourceName = datasourceName;
    this.jobType = jobType;
    this.indexName = indexName;
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
        .field("resultIndex", resultIndex)
        .field("sessionId", sessionId)
        .field(DATASOURCE_NAME, datasourceName)
        .field(JOB_TYPE, jobType.getText().toLowerCase(Locale.ROOT))
        .field(INDEX_NAME, indexName)
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
        copy.getResultIndex(),
        copy.getSessionId(),
        copy.datasourceName,
        copy.jobType,
        copy.indexName,
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

  @Override
  public String getId() {
    return queryId.docId();
  }
}
