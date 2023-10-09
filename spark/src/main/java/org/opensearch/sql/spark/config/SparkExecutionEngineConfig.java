package org.opensearch.sql.spark.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO for spark Execution Engine Config. Interface between {@link
 * org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService} and {@link
 * SparkExecutionEngineConfigSupplier}
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SparkExecutionEngineConfig {
  private String applicationId;
  private String region;
  private String executionRoleARN;
  private String sparkSubmitParameters;
  private String clusterName;
}
