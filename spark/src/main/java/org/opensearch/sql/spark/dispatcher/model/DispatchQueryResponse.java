package org.opensearch.sql.spark.dispatcher.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DispatchQueryResponse {
  private String jobId;
  private boolean isDropIndexQuery;
  private String resultIndex;
}
