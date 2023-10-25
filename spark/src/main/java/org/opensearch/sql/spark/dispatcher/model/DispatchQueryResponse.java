package org.opensearch.sql.spark.dispatcher.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;

@Data
@AllArgsConstructor
public class DispatchQueryResponse {
  private AsyncQueryId queryId;
  private String jobId;
  private boolean isDropIndexQuery;
  private String resultIndex;
  private String sessionId;
}
