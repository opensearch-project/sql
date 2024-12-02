package org.opensearch.sql.spark.dispatcher.model;

import lombok.Builder;
import lombok.Getter;
import org.opensearch.sql.spark.asyncquery.model.QueryState;

@Getter
@Builder
public class DispatchQueryResponse {
  private final String queryId;
  private final String jobId;
  private final String resultIndex;
  private final String sessionId;
  private final String datasourceName;
  private final JobType jobType;
  private final String indexName;
  private final QueryState status;
  private final String error;
}
