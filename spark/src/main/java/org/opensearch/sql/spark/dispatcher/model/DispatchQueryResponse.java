package org.opensearch.sql.spark.dispatcher.model;

import lombok.Getter;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;

@Getter
public class DispatchQueryResponse {
  private final AsyncQueryId queryId;
  private final String jobId;
  private final String resultIndex;
  private final String sessionId;
  private final String datasourceName;
  private final JobType jobType;
  private final String indexName;

  public DispatchQueryResponse(
      AsyncQueryId queryId, String jobId, String resultIndex, String sessionId) {
    this(queryId, jobId, resultIndex, sessionId, null, JobType.INTERACTIVE, null);
  }

  public DispatchQueryResponse(
      AsyncQueryId queryId,
      String jobId,
      String resultIndex,
      String sessionId,
      String datasourceName,
      JobType jobType,
      String indexName) {
    this.queryId = queryId;
    this.jobId = jobId;
    this.resultIndex = resultIndex;
    this.sessionId = sessionId;
    this.datasourceName = datasourceName;
    this.jobType = jobType;
    this.indexName = indexName;
  }
}
