package org.opensearch.sql.spark.config;

import org.opensearch.sql.spark.asyncquery.model.RequestContext;

/** Interface for extracting and providing SparkExecutionEngineConfig */
public interface SparkExecutionEngineConfigSupplier {

  /**
   * Get SparkExecutionEngineConfig
   *
   * @return {@link SparkExecutionEngineConfig}.
   */
  SparkExecutionEngineConfig getSparkExecutionEngineConfig(RequestContext requestContext);
}
