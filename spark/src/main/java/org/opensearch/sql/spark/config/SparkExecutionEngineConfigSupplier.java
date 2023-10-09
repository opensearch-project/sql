package org.opensearch.sql.spark.config;

/** Interface for extracting and providing SparkExecutionEngineConfig */
public interface SparkExecutionEngineConfigSupplier {

  /**
   * Get SparkExecutionEngineConfig
   *
   * @return {@link SparkExecutionEngineConfig}.
   */
  SparkExecutionEngineConfig getSparkExecutionEngineConfig();
}
