package org.opensearch.sql.spark.config;

import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;

/**
 * Interface for extension point to allow modification of spark submit parameter.
 * modifyParameter method is called after the default spark submit parameter is build.
 */
public interface SparkSubmitParameterModifier {
  void modifyParameters(SparkSubmitParameters parameters);
}
