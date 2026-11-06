/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import lombok.Builder;
import lombok.Data;

/**
 * POJO for spark Execution Engine Config. Interface between {@link
 * org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService} and {@link
 * SparkExecutionEngineConfigSupplier}
 */
@Data
@Builder
public class SparkExecutionEngineConfig {
  private String accountId;
  private String applicationId;
  private String region;
  private String executionRoleARN;
  private SparkSubmitParameterModifier sparkSubmitParameterModifier;
  private String clusterName;
}
