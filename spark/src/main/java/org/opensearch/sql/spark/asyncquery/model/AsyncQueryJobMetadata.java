/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.model;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import lombok.Builder.Default;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.statestore.StateModel;

/** This class models all the metadata required for a job. */
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = false)
public class AsyncQueryJobMetadata extends StateModel {
  private final AsyncQueryId queryId;
  private final String applicationId;
  private final String jobId;
  private final String resultIndex;
  // optional sessionId.
  private final String sessionId;
  // since 2.13
  // jobType could be null before OpenSearch 2.12. SparkQueryDispatcher use jobType to choose
  // cancel query handler. if jobType is null, it will invoke BatchQueryHandler.cancel().
  @Default private final JobType jobType = JobType.INTERACTIVE;
  // null if JobType is null
  private final String datasourceName;
  // null if JobType is INTERACTIVE or null
  private final String indexName;

  @Override
  public String toString() {
    return new Gson().toJson(this);
  }

  /** copy builder. update seqNo and primaryTerm */
  public static AsyncQueryJobMetadata copy(
      AsyncQueryJobMetadata copy, ImmutableMap<String, Object> metadata) {
    return builder()
        .queryId(copy.queryId)
        .applicationId(copy.getApplicationId())
        .jobId(copy.getJobId())
        .resultIndex(copy.getResultIndex())
        .sessionId(copy.getSessionId())
        .datasourceName(copy.datasourceName)
        .jobType(copy.jobType)
        .indexName(copy.indexName)
        .metadata(metadata)
        .build();
  }

  @Override
  public String getId() {
    return queryId.docId();
  }
}
