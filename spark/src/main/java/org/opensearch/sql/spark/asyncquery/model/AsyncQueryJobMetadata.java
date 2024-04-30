/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.model;

import com.google.gson.Gson;
import lombok.Data;
import lombok.EqualsAndHashCode;
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
  private final String resultLocation;
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
      AsyncQueryId queryId, String applicationId, String jobId, String resultLocation) {
    this(
        queryId,
        applicationId,
        jobId,
        resultLocation,
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
      String resultLocation,
      String sessionId) {
    this(
        queryId,
        applicationId,
        jobId,
        resultLocation,
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
      String resultLocation,
      String sessionId,
      String datasourceName,
      JobType jobType,
      String indexName) {
    this(
        queryId,
        applicationId,
        jobId,
        resultLocation,
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
      String resultLocation,
      String sessionId,
      String datasourceName,
      JobType jobType,
      String indexName,
      long seqNo,
      long primaryTerm) {
    this.queryId = queryId;
    this.applicationId = applicationId;
    this.jobId = jobId;
    this.resultLocation = resultLocation;
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

  /** copy builder. update seqNo and primaryTerm */
  public static AsyncQueryJobMetadata copy(
      AsyncQueryJobMetadata copy, long seqNo, long primaryTerm) {
    return new AsyncQueryJobMetadata(
        copy.getQueryId(),
        copy.getApplicationId(),
        copy.getJobId(),
        copy.getResultLocation(),
        copy.getSessionId(),
        copy.datasourceName,
        copy.jobType,
        copy.indexName,
        seqNo,
        primaryTerm);
  }

  @Override
  public String getId() {
    return queryId.docId();
  }
}
