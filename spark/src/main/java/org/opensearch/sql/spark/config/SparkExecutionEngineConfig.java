package org.opensearch.sql.spark.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * POJO for spark Execution Engine Config. Interface between {@link
 * org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService} and {@link
 * SparkExecutionEngineConfigSupplier}
 */
@Data
@Builder
@AllArgsConstructor
public class SparkExecutionEngineConfig {
  private String applicationId;
  private String region;
  private String executionRoleARN;
  private SparkSubmitParameterModifier sparkSubmitParameterModifier;
  private String clusterName;
}
