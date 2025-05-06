package org.opensearch.sql.spark.config;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;

/** Interface for extracting and providing SparkExecutionEngineConfig */
public interface SparkExecutionEngineConfigSupplier {

  /**
   * Get SparkExecutionEngineConfig
   *
   * @return {@link SparkExecutionEngineConfig}.
   */
  SparkExecutionEngineConfig getSparkExecutionEngineConfig(
      AsyncQueryRequestContext asyncQueryRequestContext);
}
